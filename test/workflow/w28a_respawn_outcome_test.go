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

// w28aBranchWithTail is a fork whose second branch runs prev, then billing (three
// stages), then tail; the other branch is the one-task warehouse.
func w28aBranchWithTail(billingStages []dsl.StageDef) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "warehouse", Stages: wflStages("Pack")},
				{ID: "prev", Stages: wflStages("P1")},
				{ID: "billing", Stages: billingStages},
				{ID: "tail", Stages: wflStages("T1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
					{DeptID: "prev", Steps: []dsl.ExecutionStep{{Sequential: []string{"prev", "billing"}}, {Sequential: []string{"tail"}}}},
				}},
			}},
		}},
	}
}

// A respawned branch whose resumed department ends its path (a boundary with no
// target fired) completes at that point: the steps after the department, the
// branch's tail, never run.
func TestExecute_RespawnedBranchWhoseDepartmentEndsItsPath_CompletesWithoutTheLaterSteps(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/X2")
	billing := wflStages("X1", "X2")
	billing[1].BoundaryTimer = &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, Terminates: true}
	wflRegisterAll(env, fakes, w28aBranchWithTail(billing))
	wflSignalsAt(env, 2*time.Second,
		wflSignal{"stage-transition", wflDone("warehouse", "Pack", 1)},
		wflSignal{"stage-transition", wflDone("prev", "P1", 1)},
		wflSignal{"stage-transition", wflDone("billing", "X1", 1)},
	)
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/X1", RecordVersion: 5})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/Pack", "prev/P1", "billing/X1", "billing/X2", "billing/X2"}, wflNodesCreated(fakes),
		"billing X2 was created again and the branch's tail never started")
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusRunning, domain.InstanceStatusCompleted}, wflStatuses(fakes),
		"the park ended once the branch completed at the path end")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, "billing/X2", audits[0].TargetNodeID)
}

// A respawned branch that fails again keeps the node it reached as its last
// completed node, so the next force-back resumes after that node, not at the
// node the first respawn started from.
func TestExecute_RespawnedBranchThatFailsAgain_ResumesAfterTheNodeItReached(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/X2")
	wflRegisterAll(env, fakes, w28aBranchWithTail(wflStages("X1", "X2", "X3")))
	wflSignalsAt(env, 2*time.Second,
		wflSignal{"stage-transition", wflDone("warehouse", "Pack", 1)},
		wflSignal{"stage-transition", wflDone("prev", "P1", 1)},
		wflSignal{"stage-transition", wflDone("billing", "X1", 1)},
	)
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/X1", RecordVersion: 5})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("billing", "X2", 2))
	failed := wflDone("billing", "X3", 1)
	failed.Failed, failed.Reason = true, "payment_declined"
	wflSignalAt(env, 10*time.Second, "stage-transition", failed)
	wflSignalAt(env, 14*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-4", TargetNodeKey: "billing/X2", RecordVersion: 6})
	wflSignalAt(env, 16*time.Second, "stage-transition", wflDone("billing", "X3", 2))
	wflSignalAt(env, 18*time.Second, "stage-transition", wflDone("tail", "T1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 2)
	assert.Equal(t, "billing/X2", audits[0].TargetNodeID, "the first respawn resumed at the stage after X1")
	assert.Equal(t, "billing/X3", audits[1].TargetNodeID, "the second resumed at the stage after X2, not at X2 again")
	assert.ElementsMatch(t,
		[]domain.NodeKey{"warehouse/Pack", "prev/P1", "billing/X1", "billing/X2", "billing/X2", "billing/X3", "billing/X3", "tail/T1"},
		wflNodesCreated(fakes))
}
