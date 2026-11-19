package workflow

import (
	"errors"
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errPathEnded = errors.New("workflow: path ended at an end event")

const kindInstanceTerminated = "instance-terminated"

var errInstanceTerminated = errors.New("workflow: instance cancelled")

func endsPath(f *boundaryFire) bool {
	return f.TargetDept == ""
}

type boundaryFire struct {
	Kind         string
	Interrupting bool
	TargetDept   string
	ErrorCode    string
	Steps        []dsl.ExecutionStep
}

func withSteps(steps []dsl.ExecutionStep, onFire func(boundaryFire)) func(boundaryFire) {
	return func(f boundaryFire) {
		f.Steps = steps
		onFire(f)
	}
}

func (in *interpreter) runFiredPath(ctx wf.Context, plan *dsl.CompiledPlan, f *boundaryFire) (domain.NodeKey, string, error) {
	if len(f.Steps) > 0 {
		out, err := in.runStepList(ctx, plan, f.Steps, in.admin)
		if err != nil {
			return out.LastNode, out.LastResult, err
		}
		if out.Terminated {
			return out.LastNode, out.LastResult, errInstanceTerminated
		}
		return out.LastNode, out.LastResult, errPathEnded
	}
	if endsPath(f) {
		return "", "", errPathEnded
	}
	return in.runDepartment(ctx, plan, f.TargetDept)
}

func (in *interpreter) reportTerminated(ctx wf.Context, terminated bool, err error) {
	if terminated || errors.Is(err, errInstanceTerminated) {
		in.baseAdmin.Send(ctx, adminSignalEnvelope{Kind: kindInstanceTerminated})
	}
}

// spawnFiredPath runs a non-interrupting boundary path beside its host. host
// names the host node (or sub-workflow) for the warning a failed path logs.
func (in *interpreter) spawnFiredPath(ctx wf.Context, plan *dsl.CompiledPlan, f *boundaryFire, host string) {
	if len(f.Steps) > 0 {
		wf.Go(ctx, func(gctx wf.Context) {
			out, err := in.runSteps(gctx, plan, f.Steps, in.admin)
			in.reportTerminated(gctx, out.Terminated, err)
			in.warnBoundaryPathFailed(gctx, host, err)
		})
		return
	}
	if !endsPath(f) {
		in.spawnNonInterruptingTarget(ctx, plan, f.TargetDept, host)
	}
}

// warnBoundaryPathFailed makes a failed side path visible: it never fails its
// host, so nothing else reports it. A path cut short by a cancel or an
// instance termination did not fail.
func (in *interpreter) warnBoundaryPathFailed(ctx wf.Context, host string, err error) {
	if err == nil || ctx.Err() != nil || errors.Is(err, errStageAbandoned) || errors.Is(err, errInstanceTerminated) {
		return
	}
	wf.GetLogger(ctx).Warn("non-interrupting boundary path failed",
		"tenant_id", in.tenantID, "instance_id", in.instanceID, "node_key", host, "error", err)
}

func addTimerCase(ctx wf.Context, sel wf.Selector, duration string, interrupting bool, targetDept string, onFire func(boundaryFire)) (cancel func()) {
	if duration == "" {
		return func() {}
	}
	dur, err := time.ParseDuration(duration)
	if err != nil {
		dur, err = parseISODuration(duration)
	}
	if err != nil {
		wf.GetLogger(ctx).Warn("boundary timer has an unparseable duration, disabling it", "duration", duration, "error", err)
		return func() {}
	}
	if dur <= 0 {
		wf.GetLogger(ctx).Warn("boundary timer has a non-positive duration, disabling it", "duration", duration)
		return func() {}
	}
	timerCtx, cancelTimer := wf.WithCancel(ctx)
	future := wf.NewTimer(timerCtx, dur)
	sel.AddFuture(future, func(f wf.Future) {
		if f.Get(timerCtx, nil) != nil {
			return
		}
		onFire(boundaryFire{Kind: "timer", Interrupting: interrupting, TargetDept: targetDept})
	})
	return cancelTimer
}

func addMessageCase(ctx wf.Context, sel wf.Selector, msgBuf *messageBuffer, messageName string, interrupting bool, targetDept string, onFire func(boundaryFire)) (cancel func()) {
	if messageName == "" {
		return func() {}
	}
	ch, cancelWait := msgBuf.waitChannel(ctx, messageName)
	sel.AddReceive(ch, func(c wf.ReceiveChannel, more bool) {
		var node domain.NodeKey
		c.Receive(ctx, &node)
		onFire(boundaryFire{Kind: "message", Interrupting: interrupting, TargetDept: targetDept})
	})
	return cancelWait
}

func matchErrorPath(errorPaths []dsl.ErrorPath, code string) *dsl.ErrorPath {
	var wildcard *dsl.ErrorPath
	for i := range errorPaths {
		ep := &errorPaths[i]
		if ep.ErrorCode == "" {
			if wildcard == nil {
				wildcard = ep
			}
			continue
		}
		if ep.ErrorCode == code {
			return ep
		}
	}
	return wildcard
}

func addErrorCase(ctx wf.Context, sel wf.Selector, ch wf.Channel, errorPaths []dsl.ErrorPath, onFire func(boundaryFire)) {
	if ch == nil || len(errorPaths) == 0 {
		return
	}
	sel.AddReceive(ch, func(c wf.ReceiveChannel, more bool) {
		var code string
		c.Receive(ctx, &code)
		ep := matchErrorPath(errorPaths, code)
		if ep == nil {
			wf.GetLogger(ctx).Warn("error-boundary event has no matching ErrorPath and no catch-all; dropping", "error_code", code)
			return
		}
		onFire(boundaryFire{Kind: "error", Interrupting: true, TargetDept: ep.TargetDept, ErrorCode: code, Steps: ep.Steps})
	})
}

func registerTaskBoundaries(ctx wf.Context, sel wf.Selector, in *interpreter, host string, timer *dsl.BoundaryTimer, msg *dsl.MessagePath, onFire func(boundaryFire)) (cancel func()) {
	var cancels []func()
	if timer != nil {
		cancels = append(cancels, addTimerCase(ctx, sel, timer.Duration, timer.Interrupting, timer.TargetDept, withSteps(timer.Steps, onFire)))
	}
	if msg != nil {
		cancels = append(cancels, in.addMessageBoundary(ctx, sel, host, *msg, withSteps(msg.Steps, onFire)))
	}
	return func() {
		for _, c := range cancels {
			c()
		}
	}
}

func registerSubWorkflowBoundaries(ctx wf.Context, sel wf.Selector, in *interpreter, errCh wf.Channel, sw *dsl.SubWorkflowStep, onFire func(boundaryFire)) (cancelAll func()) {
	var cancels []func()
	for _, t := range sw.TimerPaths {
		cancels = append(cancels, addTimerCase(ctx, sel, t.Duration, t.Interrupting, t.TargetDept, withSteps(t.Steps, onFire)))
	}
	for _, m := range sw.MessagePaths {
		cancels = append(cancels, in.addMessageBoundary(ctx, sel, sw.NodeID, m, withSteps(m.Steps, onFire)))
	}
	addErrorCase(ctx, sel, errCh, sw.ErrorPaths, onFire)
	return func() {
		for _, c := range cancels {
			c()
		}
	}
}
