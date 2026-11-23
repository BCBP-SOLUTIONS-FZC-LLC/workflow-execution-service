package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func wflQueryForceRouteStateAt(t *testing.T, env *testsuite.TestWorkflowEnvironment, delay time.Duration, into *port.ForceRouteState) {
	t.Helper()
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(port.QueryForceRouteState)
		if err != nil {
			t.Errorf("QueryWorkflow(%s): %v", port.QueryForceRouteState, err)
			return
		}
		if err := val.Get(into); err != nil {
			t.Errorf("decoding %s: %v", port.QueryForceRouteState, err)
		}
	}, delay)
}

// The service asks the workflow, before it signals a force-route, which
// branches have failed and how many completed nodes a force-back can reach.
func TestExecute_ForceRouteStateQueryReportsFailedBranchesAndHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		if ct, ok := in.(port.CreateTaskInput); ok && name == port.ActivityCreateTask && ct.NodeKey == "billing/prep" {
			return temporal.NewApplicationError("card declined", "ValidationError")
		}
		return nil
	}
	wflRegisterAll(env, fakes, parallelCollaboration())

	var state port.ForceRouteState
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflQueryForceRouteStateAt(t, env, 5*time.Second, &state)
	wflSignalAt(env, 10*time.Second, "instance-force-forward", adminSignalWire{
		AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
	})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []port.FailedBranchState{{DeptID: "billing", Covers: []string{"billing"}}}, state.FailedBranches)
}

func TestExecute_ForceRouteStateQueryCountsTheNodeHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, nestedParallelCollaboration())

	var state port.ForceRouteState
	wflQueryForceRouteStateAt(t, env, time.Second, &state)
	wflSignalAt(env, 5*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})

	_, _ = wflStart(t, env, fakes, "")

	assert.Empty(t, state.FailedBranches)
	assert.Zero(t, state.HistoryLen, "no node has completed yet")
}
