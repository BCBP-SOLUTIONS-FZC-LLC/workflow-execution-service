package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// wflFailTaskOnce makes the first creation of the task for node fail with a
// non-retryable error and every later one succeed.
func wflFailTaskOnce(fakes *wflFakes, node domain.NodeKey) {
	failed := false
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && v.NodeKey == node && !failed {
			failed = true
			return wflRefuse("create " + string(node))
		}
		return nil
	}
}

// wflTwoDeptBranch is a fork whose second branch runs "prev" and then
// "billing" (two stages) in sequence, next to a one-task "warehouse" branch.
func wflTwoDeptBranch() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "warehouse", Stages: wflStages("Pack")},
				{ID: "prev", Stages: wflStages("P1")},
				{ID: "billing", Stages: wflStages("X1", "X2")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
					{DeptID: "prev", Steps: []dsl.ExecutionStep{{Sequential: []string{"prev", "billing"}}}},
				}},
			}},
		}},
	}
}

// wflRunTwoDeptBranchRecovery fails billing's second stage once, force-backs
// the failed branch and finishes the run.
func wflRunTwoDeptBranchRecovery(t *testing.T) *wflFakes {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/X2")
	wflRegisterAll(env, fakes, wflTwoDeptBranch())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("prev", "P1", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("billing", "X1", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/X1", RecordVersion: 5})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("billing", "X2", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	return fakes
}

// A branch that fails partway through a department is recorded as failed at
// that department's own last finished stage, and after a force-back it resumes
// at the stage that failed: the stages already finished are not run again.
func TestExecute_BranchFailingPartwayThroughADepartmentIsParkedAtItsOwnLastFinishedStage(t *testing.T) {
	fakes := wflRunTwoDeptBranchRecovery(t)

	var degraded []port.UpdateInstanceStatusInput
	for _, w := range wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus) {
		if w.Status == domain.InstanceStatusDegraded {
			degraded = append(degraded, w)
		}
	}
	require.Len(t, degraded, 1)
	assert.Equal(t, wflTenant, degraded[0].TenantID)
	assert.Equal(t, wflInstance, degraded[0].InstanceID)
	require.Len(t, degraded[0].FailedBranches, 1)
	assert.Equal(t, "billing/X1", degraded[0].FailedBranches[0].LastNodeKey)
	assert.ElementsMatch(t,
		[]domain.NodeKey{"warehouse/Pack", "prev/P1", "billing/X1", "billing/X2", "billing/X2"},
		wflNodesCreated(fakes), "the finished first stage of billing was not created again")
}

// The force-route record of a force-back on a failed branch names the node the
// branch resumes at, the stage that failed, not the last stage it finished.
func TestExecute_ForceBackOnAFailedBranchRecordsTheNodeTheBranchResumesAt(t *testing.T) {
	fakes := wflRunTwoDeptBranchRecovery(t)

	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, wflInstance, audits[0].InstanceID)
	assert.Equal(t, domain.ForceRouteDirectionBack, audits[0].Direction)
	assert.Equal(t, "billing/X2", audits[0].TargetNodeID)
	assert.Equal(t, "admin-3", audits[0].AdminUserID)
}

// A branch that finished nothing before failing is rerun from its first
// department, and the force-route record names that department's first stage.
func TestExecute_ForceBackOnABranchThatFinishedNothingRecordsItsFirstStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "prev/P1")
	wflRegisterAll(env, fakes, wflTwoDeptBranch())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "prev/P1", RecordVersion: 5})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("prev", "P1", 2))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "X1", 1))
	wflSignalAt(env, 14*time.Second, "stage-transition", wflDone("billing", "X2", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, "prev/P1", audits[0].TargetNodeID)
}

// wflGatewayBranch is a fork whose second branch runs "intake", an exclusive
// gateway that leads to "review", and then "billing".
func wflGatewayBranch() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "warehouse", Stages: wflStages("Pack")},
				{ID: "intake", Stages: wflStages("I1")},
				{ID: "review", Stages: wflStages("R1")},
				{ID: "billing", Stages: wflStages("B1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
					{DeptID: "intake", Steps: []dsl.ExecutionStep{
						{Sequential: []string{"intake"}},
						{Exclusive: []dsl.ExclusiveBranch{{ConditionExpression: `decision == "go"`, Target: "review"}}},
						{Sequential: []string{"billing"}},
					}},
				}},
			}},
		}},
	}
}

// A failed branch whose last finished department was reached through an
// exclusive gateway is respawned past that department: the department that
// failed after it runs again, the force-route record names it, and the park
// does not count the branch as recovered without that work.
func TestExecute_RespawnedBranchFindsItsDepartmentThroughAGatewayTarget(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/B1")
	wflRegisterAll(env, fakes, wflGatewayBranch())
	intake := wflDone("intake", "I1", 1)
	intake.ResultJSON = `{"decision":"go"}`
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", intake)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("review", "R1", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/B1", RecordVersion: 5})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.ElementsMatch(t,
		[]domain.NodeKey{"warehouse/Pack", "intake/I1", "review/R1", "billing/B1", "billing/B1"},
		wflNodesCreated(fakes), "billing ran again after the force-back")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, "billing/B1", audits[0].TargetNodeID)
}

// A force-back on a failed branch whose force-route record is refused changes
// nothing: the branch stays parked, no task is created again, and a later
// cancel still ends the instance.
func TestExecute_ForceBackOnAFailedBranchWhoseAuditWriteIsRefusedLeavesTheBranchParked(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	failed := false
	fakes.failFor = func(name string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && v.NodeKey == "billing/X2" && !failed {
			failed = true
			return wflRefuse("create billing/X2")
		}
		if name == port.ActivityRecordForceRoute {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflTwoDeptBranch())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("prev", "P1", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("billing", "X1", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/X1", RecordVersion: 5})
	wflCancelAt(env, 10*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/Pack", "prev/P1", "billing/X1", "billing/X2"}, wflNodesCreated(fakes),
		"the branch was not respawned")
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityRecordForceRoute))
}
