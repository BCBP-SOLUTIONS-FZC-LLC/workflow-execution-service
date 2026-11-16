package workflow

import (
	"errors"
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func (in *interpreter) runSubWorkflow(ctx wf.Context, plan *dsl.CompiledPlan, sw *dsl.SubWorkflowStep, admin wf.Channel) (domain.NodeKey, error) {
	doneCh := wf.NewBufferedChannel(ctx, 1)
	errCh := wf.NewBufferedChannel(ctx, 1)

	var lastNode domain.NodeKey
	var runErr error
	childCtx, cancelChild := wf.WithCancel(ctx)
	wf.Go(childCtx, func(gctx wf.Context) {
		out, err := in.runSteps(gctx, plan, sw.Plan.Steps, admin)
		lastNode, runErr = out.LastNode, err
		if out.Terminated && err == nil {
			runErr = errInstanceTerminated
		}
		if code, raised := in.raisedErrorCode(gctx, sw, err); raised {
			errCh.Send(gctx, code)
			return
		}
		doneCh.Send(gctx, struct{}{})
	})

	sel := wf.NewSelector(ctx)
	var completed bool
	sel.AddReceive(doneCh, func(c wf.ReceiveChannel, more bool) {
		var v struct{}
		c.Receive(ctx, &v)
		completed = true
	})

	var fired *boundaryFire
	cancelBoundaries := onceOnly(registerSubWorkflowBoundaries(ctx, sel, in, errCh, sw, func(f boundaryFire) {
		fired = &f
	}))
	defer cancelBoundaries()

	for {
		for !completed && fired == nil {
			sel.Select(ctx)
			if !completed && fired == nil && ctx.Err() != nil {
				return lastNode, errStageAbandoned
			}
		}
		if completed {
			return lastNode, runErr
		}

		f := fired
		fired = nil
		if f.Interrupting {
			cancelBoundaries()
			cancelChild()
			if f.Kind != "error" {
				in.awaitWindDown(ctx, doneCh)
			}
			node, _, err := in.runFiredPath(ctx, plan, f)
			return node, err
		}
		in.spawnFiredPath(ctx, plan, f)
	}
}

func onceOnly(f func()) func() {
	done := false
	return func() {
		if !done {
			done = true
			f()
		}
	}
}

const windDownWait = 30 * time.Second

func (in *interpreter) awaitWindDown(ctx wf.Context, doneCh wf.ReceiveChannel) {
	if getVersion(ctx, abandonedTaskSupersededChangeID) == wf.DefaultVersion {
		return
	}
	timerCtx, cancelTimer := wf.WithCancel(ctx)
	defer cancelTimer()
	sel := wf.NewSelector(ctx)
	sel.AddReceive(doneCh, func(c wf.ReceiveChannel, more bool) {
		var v struct{}
		c.Receive(ctx, &v)
	})
	sel.AddFuture(wf.NewTimer(timerCtx, windDownWait), func(wf.Future) {})
	sel.Select(ctx)
}

func (in *interpreter) raisedErrorCode(ctx wf.Context, sw *dsl.SubWorkflowStep, err error) (string, bool) {
	var failed *stageFailedError
	if !errors.As(err, &failed) || matchErrorPath(sw.ErrorPaths, failed.Reason) == nil {
		return "", false
	}
	if getVersion(ctx, subWorkflowErrorRaisedChangeID) == wf.DefaultVersion {
		return "", false
	}
	return failed.Reason, true
}
