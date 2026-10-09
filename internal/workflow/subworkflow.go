package workflow

import (
	"errors"
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
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
	cancelBoundaries := registerSubWorkflowBoundaries(ctx, sel, in, errCh, sw, func(f boundaryFire) {
		fired = &f
	})

	sel.Select(ctx)

	if completed {
		cancelBoundaries()
		return lastNode, runErr
	}

	if fired.Interrupting {
		cancelChild()
		if fired.Kind != "error" {
			in.awaitWindDown(ctx, doneCh)
		}
		node, _, err := in.runFiredPath(ctx, plan, fired)
		return node, err
	}

	in.spawnFiredPath(ctx, plan, fired)
	var v struct{}
	doneCh.Receive(ctx, &v)
	return lastNode, runErr
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
