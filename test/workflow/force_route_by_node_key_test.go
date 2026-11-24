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
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
)

func wflQueryWaitingAt(t *testing.T, env *testsuite.TestWorkflowEnvironment, delay time.Duration, into *[]string, taken *bool) {
	t.Helper()
	wflclock.After(env, func() {
		val, err := env.QueryWorkflow(port.QueryWaitingExternal)
		if err != nil {
			t.Errorf("QueryWorkflow(%s): %v", port.QueryWaitingExternal, err)
			return
		}
		if err := val.Get(into); err != nil {
			t.Errorf("decoding %s: %v", port.QueryWaitingExternal, err)
			return
		}
		*taken = true
	}, delay)
}

func wflQueryStatusAt(t *testing.T, env *testsuite.TestWorkflowEnvironment, delay time.Duration, into *wfengine.WorkflowStatusQuery) {
	t.Helper()
	wflclock.After(env, func() {
		val, err := env.QueryWorkflow(wfengine.QueryGetWorkflowStatus)
		if err != nil {
			t.Errorf("QueryWorkflow(%s): %v", wfengine.QueryGetWorkflowStatus, err)
			return
		}
		if err := val.Get(into); err != nil {
			t.Errorf("decoding %s: %v", wfengine.QueryGetWorkflowStatus, err)
		}
	}, delay)
}

// A force-forward that names its target by node key only reaches a parallel
// step and a degraded park: the department comes from the key.
func TestExecute_ForceForwardNamingOnlyANodeKeyIsTakenByAParallelStepAndADegradedPark(t *testing.T) {
	t.Run("parallel step open", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		collab := parallelCollaboration()
		fakes := wflNewFakes()
		wflRegisterAll(env, fakes, collab)

		wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{
			AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
		})
		wflSignalAt(env, 20*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status)
		routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
		require.Len(t, routes, 1)
		assert.Equal(t, port.RecordForceRouteInput{
			InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"billing/prep"},
			TargetNodeID: "billing/prep", Direction: domain.ForceRouteDirectionForward, AdminUserID: "admin-1", RecordVersion: 7,
		}, routes[0])
	})

	t.Run("degraded park", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		collab := parallelCollaboration()
		fakes := wflNewFakes()
		fakes.failFor = func(name string, in any) error {
			if ct, ok := in.(port.CreateTaskInput); ok && name == port.ActivityCreateTask && ct.NodeKey == "billing/prep" {
				return temporal.NewApplicationError("card declined", "ValidationError")
			}
			return nil
		}
		wflRegisterAll(env, fakes, collab)

		wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
		wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{
			AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
		})

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status)
		routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
		require.Len(t, routes, 1)
		assert.Equal(t, "billing/prep", routes[0].TargetNodeID)
		assert.Equal(t, wflTenant, routes[0].TenantID)
		assert.Equal(t, wflInstance, routes[0].InstanceID)
		assert.EqualValues(t, 7, routes[0].RecordVersion)
	})
}

// A degraded park finds the failed branch by node key: a force-forward naming
// no target takes the only failed branch, and a force-back naming only a node
// key respawns it.
func TestExecute_DegradedParkFindsTheFailedBranchByNodeKeyOrAsTheOnlyOne(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := parallelCollaboration()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		if ct, ok := in.(port.CreateTaskInput); ok && name == port.ActivityCreateTask && ct.NodeKey == "billing/prep" && ct.VisitCount == 1 {
			return temporal.NewApplicationError("card declined", "ValidationError")
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)

	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{
		AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7,
	})
	wflSignalAt(env, 20*time.Second, "stage-transition", wflDoneStage("billing", "prep", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, domain.ForceRouteDirectionBack, routes[0].Direction)
	assert.EqualValues(t, 7, routes[0].RecordVersion)
	assert.Equal(t, wflTenant, routes[0].TenantID)
	assert.Equal(t, wflInstance, routes[0].InstanceID)
}
