package workflow

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestRunCallPoolIgnoredVisitsGetDistinctNodeKeys is regression coverage for
// a bug where two occurrences of the same Ignored CallPool target (e.g. from
// two concurrent Parallel branches) resolved to the identical NodeKey
// ("call_pool/"+poolName) — the second registration in stage.go's in.pending
// would silently clobber the first's still-live entry. Each visit must get
// its own, distinct NodeKey.
func TestRunCallPoolIgnoredVisitsGetDistinctNodeKeys(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceNodesInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			return port.CompleteAssignmentOutput{AllDone: true}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "call_pool", NodeID: "vendor-pool#1", ResultJSON: "{}"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "call_pool", NodeID: "vendor-pool#2", ResultJSON: "{}"})
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		collab := &dsl.CompiledCollaboration{
			MainPlan: "main",
			Plans: []*dsl.CompiledPlan{
				{Name: "main"},
				{Name: "vendor-pool", Ignored: true},
			},
		}
		in := newInterpreter("tenant", "instance", "", collab)
		admin := wf.NewBufferedChannel(ctx, 2)
		baseAdmin := wf.NewBufferedChannel(ctx, 2)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		callerPlan := &dsl.CompiledPlan{Name: "main"}
		node1, err := in.runCallPool(ctx, callerPlan, &dsl.CallPoolStep{Pool: "vendor-pool"}, admin)
		if err != nil {
			return err
		}
		node2, err := in.runCallPool(ctx, callerPlan, &dsl.CallPoolStep{Pool: "vendor-pool"}, admin)
		if err != nil {
			return err
		}
		if node1 == node2 {
			return errBadHistoryLen(-4)
		}
		if node1 != "call_pool/vendor-pool#1" || node2 != "call_pool/vendor-pool#2" {
			return errBadHistoryLen(-5)
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}
