package workflow

import (
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// boundaryFire describes which boundary fired first, if any, when racing a
// host step's own completion against its attached boundary events (LLD
// §2.2's 5-step runtime algorithm).
type boundaryFire struct {
	Kind         string // "timer", "message", or "error"
	Interrupting bool
	TargetDept   string
	ErrorCode    string // set only for Kind == "error"
}

// addTimerCase registers a boundary timer as an additional Selector case.
// Returns a cancel func the caller must invoke once the race resolves, so an
// unfired timer doesn't leak (LLD §2.2 step 4: "if the host's own completion
// fires first, any pending timer is cancelled").
func addTimerCase(ctx wf.Context, sel wf.Selector, duration string, interrupting bool, targetDept string, onFire func(boundaryFire)) (cancel func()) {
	if duration == "" {
		return func() {}
	}
	dur, err := time.ParseDuration(duration)
	if err != nil {
		return func() {}
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
// same buffer plain send_task/receive_task stages use (LLD §2.2/§2.4).
// Returns a cancel func the caller must invoke if the race resolves some
// other way, so this boundary's queued waiter doesn't stay behind to
// intercept a delivery meant for a later, still-live waiter on the same
// message name.
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

// addErrorCase registers a subProcess error boundary as an additional
// Selector case. ch is pushed to by the recursive subprocess call on a
// matching ErrorCode (LLD §2.2 step 3). Error boundaries are only legal on
// subProcess — never a plain task (compile-time rejected upstream).
func addErrorCase(ctx wf.Context, sel wf.Selector, ch wf.Channel, errorPaths []dsl.ErrorPath, onFire func(boundaryFire)) {
	if ch == nil || len(errorPaths) == 0 {
		return
	}
	sel.AddReceive(ch, func(c wf.ReceiveChannel, more bool) {
		var code string
		c.Receive(ctx, &code)
		for _, ep := range errorPaths {
			if ep.ErrorCode == code || ep.ErrorCode == "" {
				onFire(boundaryFire{Kind: "error", TargetDept: ep.TargetDept, ErrorCode: code})
				return
			}
		}
	})
}

// registerTaskBoundaries wires a plain task's at-most-one timer and
// at-most-one message boundary onto sel (LLD §2.2's cardinality table: a
// plain task carries singular boundaries, compiler-enforced).
func registerTaskBoundaries(ctx wf.Context, sel wf.Selector, msgBuf *messageBuffer, timer *dsl.BoundaryTimer, msg *dsl.MessagePath, onFire func(boundaryFire)) (cancel func()) {
	var cancels []func()
	if timer != nil {
		cancels = append(cancels, addTimerCase(ctx, sel, timer.Duration, timer.Interrupting, timer.TargetDept, onFire))
	}
	if msg != nil {
		cancels = append(cancels, addMessageCase(ctx, sel, msgBuf, msg.MessageName, msg.Interrupting, msg.TargetDept, onFire))
	}
	return func() {
		for _, c := range cancels {
			c()
		}
	}
}

// registerSubWorkflowBoundaries wires a subProcess's timer/error/message
// boundary arrays onto sel (LLD §2.2's cardinality table: subProcess carries
// arrays, since multiple boundaries of the same kind may attach to one
// subprocess).
func registerSubWorkflowBoundaries(ctx wf.Context, sel wf.Selector, msgBuf *messageBuffer, errCh wf.Channel, sw *dsl.SubWorkflowStep, onFire func(boundaryFire)) (cancelAll func()) {
	var cancels []func()
	for _, t := range sw.TimerPaths {
		cancels = append(cancels, addTimerCase(ctx, sel, t.Duration, t.Interrupting, t.TargetDept, onFire))
	}
	for _, m := range sw.MessagePaths {
		cancels = append(cancels, addMessageCase(ctx, sel, msgBuf, m.MessageName, m.Interrupting, m.TargetDept, onFire))
	}
	addErrorCase(ctx, sel, errCh, sw.ErrorPaths, onFire)
	return func() {
		for _, c := range cancels {
			c()
		}
	}
}
