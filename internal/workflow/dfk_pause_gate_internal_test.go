package workflow

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// A branch waiting at a pause gate whose run is then cancelled does not run its
// next stage when the gate opens: no message is sent and nothing is pushed to
// the history.
func TestCheckPausedEndsAsAbandonedWhenTheRunIsCancelledAtTheGate(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	plan := &dsl.CompiledPlan{
		Name: "main",
		Departments: []dsl.DepartmentDef{{ID: "d", Stages: []dsl.StageDef{
			{Type: "send_task", NodeID: "Notify", Extras: map[string]string{"message": "note"}},
		}}},
	}
	in := newInterpreter("tenant", "instance", "", nil, nil)
	var runErr error

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in.pauseDept(ctx, "d")
		runCtx, cancelRun := wf.WithCancel(ctx)
		done := wf.NewChannel(ctx)
		wf.Go(runCtx, func(gctx wf.Context) {
			_, _, runErr = in.runDepartment(gctx, plan, "d")
			done.Send(gctx, struct{}{})
		})
		if err := wf.Sleep(ctx, time.Second); err != nil {
			return err
		}
		cancelRun()
		in.resumeDept("d")
		done.Receive(ctx, nil)
		return nil
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !errors.Is(runErr, errStageAbandoned) {
		t.Errorf("runDepartment error = %v, want the abandoned error", runErr)
	}
	if got := in.msgBuf.buf["note"]; len(got) != 0 {
		t.Errorf("message buffer holds %v, want no message sent from a cancelled run", got)
	}
	if got := in.history.Peek(); got != "" {
		t.Errorf("history top = %q, want it unchanged", got)
	}
}
