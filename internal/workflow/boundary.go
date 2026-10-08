package workflow

import (
	"errors"
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// errPathEnded means a path reached its end event: an interrupting boundary
// with no target department fired, or an exclusive branch that terminates
// was taken. The steps of the plan level it belongs to stop there. runSteps
// turns it into a normal finish, so a call's own flow ends and its caller
// carries on, and a top-level path ends the instance.
var errPathEnded = errors.New("workflow: path ended at an end event")

// endsPath reports whether f ends its path rather than moving to a
// department.
func endsPath(ctx wf.Context, f *boundaryFire) bool {
	return getVersion(ctx, boundaryEndsPathChangeID) != wf.DefaultVersion && f.TargetDept == ""
}

type boundaryFire struct {
	Kind         string // "timer", "message", or "error"
	Interrupting bool
	TargetDept   string
	ErrorCode    string // set only for Kind == "error"
	// Steps is the whole path the boundary leads to; empty in a plan compiled
	// before paths were carried, whose boundary names only TargetDept.
	Steps []dsl.ExecutionStep
}

// withSteps adds the path of the boundary being registered to what onFire
// is told when it fires.
func withSteps(steps []dsl.ExecutionStep, onFire func(boundaryFire)) func(boundaryFire) {
	return func(f boundaryFire) {
		f.Steps = steps
		onFire(f)
	}
}

// runFiredPath runs what an interrupting boundary leads to, in the host's
// place: its Steps in order, after which the path ends, so nothing the host
// would have gone on to runs. Without Steps it runs TargetDept's department
// alone, or ends the path when there is none.
func (in *interpreter) runFiredPath(ctx wf.Context, plan *dsl.CompiledPlan, f *boundaryFire) (domain.NodeKey, string, error) {
	if len(f.Steps) > 0 {
		out, err := in.runStepList(ctx, plan, f.Steps, in.admin)
		if err != nil {
			return out.LastNode, out.LastResult, err
		}
		return out.LastNode, out.LastResult, errPathEnded
	}
	if endsPath(ctx, f) {
		return "", "", errPathEnded
	}
	return in.runDepartment(ctx, plan, f.TargetDept)
}

// spawnFiredPath runs the path of a non-interrupting boundary beside its
// host.
func (in *interpreter) spawnFiredPath(ctx wf.Context, plan *dsl.CompiledPlan, f *boundaryFire) {
	if len(f.Steps) > 0 {
		wf.Go(ctx, func(gctx wf.Context) {
			_, _ = in.runSteps(gctx, plan, f.Steps, in.admin)
		})
		return
	}
	if !endsPath(ctx, f) {
		in.spawnNonInterruptingTarget(ctx, plan, f.TargetDept)
	}
}

func addTimerCase(ctx wf.Context, sel wf.Selector, duration string, interrupting bool, targetDept string, onFire func(boundaryFire)) (cancel func()) {
	iso := getVersion(ctx, isoTimerDurationChangeID) != wf.DefaultVersion
	if duration == "" {
		return func() { /* no boundary was registered, nothing to cancel */ }
	}
	dur, err := time.ParseDuration(duration)
	if err != nil && iso {
		dur, err = parseISODuration(duration)
	}
	if err != nil {
		wf.GetLogger(ctx).Warn("boundary timer has an unparseable duration, disabling it", "duration", duration, "error", err)
		return func() { /* no boundary was registered, nothing to cancel */ }
	}
	timerCtx, cancelTimer := wf.WithCancel(ctx)
	future := wf.NewTimer(timerCtx, dur)
	sel.AddFuture(future, func(f wf.Future) {
		if f.Get(timerCtx, nil) != nil {
			return // cancelled, not fired
		}
		onFire(boundaryFire{Kind: "timer", Interrupting: interrupting, TargetDept: targetDept})
	})
	return cancelTimer
}

// addMessageCase registers a boundary message as an additional Selector
// case, consuming from the interpreter's instance-wide message buffer — the
// same buffer plain send_task/receive_task stages use (LLD §2.2/§2.4). The
// returned cancel func's contract is message_buffer.go's waitChannel.
func addMessageCase(ctx wf.Context, sel wf.Selector, msgBuf *messageBuffer, messageName string, interrupting bool, targetDept string, onFire func(boundaryFire)) (cancel func()) {
	if messageName == "" {
		return func() { /* no boundary was registered, nothing to cancel */ }
	}
	ch, cancelWait := msgBuf.waitChannel(ctx, messageName)
	sel.AddReceive(ch, func(c wf.ReceiveChannel, more bool) {
		var node domain.NodeKey
		c.Receive(ctx, &node)
		onFire(boundaryFire{Kind: "message", Interrupting: interrupting, TargetDept: targetDept})
	})
	return cancelWait
}

// matchErrorPath finds the ErrorPath matching code, preferring an exact
// ErrorCode match over the implicit-else (ErrorCode == "") catch-all
// regardless of list order — the same order-independent precedence
// selectBranch (exclusive.go) already applies to its own single-wildcard
// convention.
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
