package workflow

import (
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

type boundaryFire struct {
	Kind         string // "timer", "message", or "error"
	Interrupting bool
	TargetDept   string
	ErrorCode    string // set only for Kind == "error"
}

func addTimerCase(ctx wf.Context, sel wf.Selector, duration string, interrupting bool, targetDept string, onFire func(boundaryFire)) (cancel func()) {
	if duration == "" {
		return func() { /* no boundary was registered, nothing to cancel */ }
	}
	dur, err := time.ParseDuration(duration)
	if err != nil {
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
