package workflow

import (
	"errors"
	"time"

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
	// errCh is sent the error class of a stage that fails inside the body when
	// one of the error paths matches it. Buffered, like doneCh, so the body's
	// goroutine never waits on a selector that has already moved on.
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
		if fired.Kind != "error" { // an error is raised by a body that has already finished
			in.awaitWindDown(ctx, doneCh)
		}
		node, _, err := in.runFiredPath(ctx, plan, fired)
		return node, err
	}

	// Non-interrupting: both continue independently (LLD §2.2 step 5).
	in.spawnFiredPath(ctx, plan, fired)
	var v struct{}
	doneCh.Receive(ctx, &v)
	return lastNode, runErr
}

// windDownWait bounds how long an interrupted scope is given to mark its open
// tasks SUPERSEDED. A body holding a wait that ignores cancellation, a receive
// task or a parallel step, never winds down and is simply left.
const windDownWait = 30 * time.Second

// awaitWindDown waits for the cancelled body of a scope to finish, which it
// does once every stage it holds has written its SUPERSEDED status, so the
// workflow cannot end first and leave those tasks open.
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

// raisedErrorCode reports the error class a failed stage raises to sw's error
// boundaries: the class the stage failed with, when one of sw's error paths
// matches it. A failure no path matches fails the instance as before, and so
// does any other error.
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
