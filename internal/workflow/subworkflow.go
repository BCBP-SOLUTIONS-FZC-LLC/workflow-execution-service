package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// runSubWorkflow interprets a subProcess inline, recursively, in the same
// Temporal execution — never a child workflow (execution LLD §2.3) — racing
// its own completion against the subprocess's boundary events (§2.2).
func (in *interpreter) runSubWorkflow(ctx wf.Context, plan *dsl.CompiledPlan, sw *dsl.SubWorkflowStep, admin wf.Channel) (domain.NodeKey, error) {
	// Buffered: the interrupting-boundary path below returns without ever
	// receiving from doneCh, so the child goroutine's own Send must not
	// block on a listener that's gone — an unbuffered channel here leaked
	// one coroutine per interrupting boundary fired, for the rest of the
	// workflow execution.
	doneCh := wf.NewBufferedChannel(ctx, 1)
	// errCh: pushed to on a matching ErrorCode. Nothing pushes to it yet —
	// error-raising activities are a sibling task's concern.
	errCh := wf.NewChannel(ctx)

	var lastNode domain.NodeKey
	var runErr error
	childCtx, cancelChild := wf.WithCancel(ctx)
	wf.Go(childCtx, func(gctx wf.Context) {
		out, err := in.runSteps(gctx, plan, sw.Plan.Steps, admin)
		lastNode, runErr = out.LastNode, err
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
	cancelBoundaries := registerSubWorkflowBoundaries(ctx, sel, in.msgBuf, errCh, sw, func(f boundaryFire) {
		fired = &f
	})

	sel.Select(ctx)

	if completed {
		cancelBoundaries()
		return lastNode, runErr
	}

	if fired.Interrupting {
		cancelChild()
		return in.runDepartment(ctx, plan, fired.TargetDept)
	}

	// Non-interrupting: both continue independently (LLD §2.2 step 5).
	in.spawnNonInterruptingTarget(ctx, plan, fired.TargetDept)
	var v struct{}
	doneCh.Receive(ctx, &v)
	return lastNode, runErr
}
