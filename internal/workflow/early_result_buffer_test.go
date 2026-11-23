package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestDeliverResult_AResultForANodeNobodyWaitsOnIsKeptOnePerNodeAndVisit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.status = domain.InstanceStatusRunning
		first := nodeVisitKey{Node: "ops/Step", Visit: 1}
		in.deliverResult(ctx, SignalStageTransition, first, stageTransitionSignal{ResultJSON: `{"n":1}`})
		in.deliverResult(ctx, SignalStageTransition, first, stageTransitionSignal{ResultJSON: `{"n":2}`})
		in.deliverResult(ctx, SignalStageTransition, nodeVisitKey{Node: "ops/Step", Visit: 2}, stageTransitionSignal{ResultJSON: `{"n":3}`})

		require.Len(t, in.pendingSignals, 2, "one entry per node and visit")
		assert.Equal(t, `{"n":2}`, in.pendingSignals[first].ResultJSON, "a second result for the same node and visit replaces the first")
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
}
