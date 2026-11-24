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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflForkAfterIntake() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "intake", Stages: wflStages("A1")},
				{ID: "warehouse", Stages: wflStages("Pack")},
				{ID: "billing", Stages: wflStages("B1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"intake"}},
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
					{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
				}},
			}},
		}},
	}
}

func wflDeferTask(env *testsuite.TestWorkflowEnvironment, update string, delay time.Duration, taskID string, answer *updateAnswer) {
	wflclock.After(env, func() {
		env.UpdateWorkflow("stage-defer:"+wflInstance, update, answer, deferWire{
			DeptID: "billing", FromStage: "B1", Reason: "need more info", UserID: "user-1", RecordVersion: 1,
			TaskID: taskID, AssignmentID: "assign-1",
		})
	}, delay)
}

// wflParkWithRespawnedBilling brings the instance to a park whose failed
// billing branch has been force-backed, so billing's task is open again.
// intake finishes at 2s, warehouse at 4s, billing's first task is refused, and
// the force-back at 6s respawns billing.
func wflParkWithRespawnedBilling(env *testsuite.TestWorkflowEnvironment, fakes *wflFakes) {
	wflFailTaskOnce(fakes, "billing/B1")
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/B1", RecordVersion: 5})
}

// Deferring the open task of a respawned branch in a park returns the work to
// the step before the fork: the respawn is abandoned and its branch is failed
// again, the step runs again, nothing stays paused, and the park still ends
// once the branch is resolved.
func TestExecute_DeferOfARespawnedBranchTaskInAParkReturnsToTheStepBeforeTheFork(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflParkWithRespawnedBilling(env, fakes)
	answer := &updateAnswer{}
	var rerunOpen, afterRerun wfengine.WorkflowStatusQuery
	wflDeferTask(env, "defer-1", 8*time.Second, "billing/B1", answer)
	wflQueryStatusAt(t, env, 9*time.Second, &rerunOpen)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflQueryStatusAt(t, env, 11*time.Second, &afterRerun)
	wflSignalAt(env, 12*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/B1", RecordVersion: 6})
	wflSignalAt(env, 14*time.Second, "stage-transition", wflDone("billing", "B1", 3))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.True(t, answer.completed && answer.err == nil && answer.rejected == nil, "the defer was applied: %+v", answer)
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, wflTenant, deferred[0].TenantID)
	assert.Equal(t, "billing/B1", deferred[0].TaskID)
	assert.Equal(t, "intake/A1", deferred[0].DeferredToNodeKey)
	assert.ElementsMatch(t,
		[]domain.NodeKey{"intake/A1", "warehouse/Pack", "billing/B1", "billing/B1", "intake/A1", "billing/B1"},
		wflNodesCreated(fakes), "the step before the fork ran again, and billing ran again only after its own force-back")
	assert.Equal(t, domain.InstanceStatusDegraded, rerunOpen.Status)
	assert.Equal(t, []domain.NodeKey{"intake/A1"}, rerunOpen.ActiveTasks, "the abandoned respawn holds no task open")
	assert.Equal(t, domain.InstanceStatusDegraded, afterRerun.Status, "the failed branch is still parked")
	assert.Empty(t, afterRerun.ActiveTasks)
	assert.Empty(t, afterRerun.SavedSiblingBranches, "no department is left paused")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 2, "the defer routes without an admin record; the two force-backs are recorded")
	for _, a := range audits {
		assert.Equal(t, domain.ForceRouteDirectionBack, a.Direction)
		assert.Equal(t, "billing/B1", a.TargetNodeID)
	}
}

// A cancel that arrives while the step before the fork is running again ends
// the instance: the park keeps reading admin signals during the rerun.
func TestExecute_CancelWhileAParkRerunsTheStepBeforeTheForkTerminatesTheInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflParkWithRespawnedBilling(env, fakes)
	wflDeferTask(env, "defer-1", 8*time.Second, "billing/B1", &updateAnswer{})
	wflCancelAt(env, 10*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityCancelInstance))
}

// Deferring the task the rerun itself created does not hang the park: the
// rerun is abandoned and the step runs once more, and the instance goes on.
func TestExecute_DeferOfTheRerunTaskInAParkRunsTheStepAgain(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflParkWithRespawnedBilling(env, fakes)
	first, second := &updateAnswer{}, &updateAnswer{}
	wflDeferTask(env, "defer-1", 8*time.Second, "billing/B1", first)
	wflDeferTask(env, "defer-2", 10*time.Second, "intake/A1", second)
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("intake", "A1", 3))
	wflSignalAt(env, 14*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/B1", RecordVersion: 6})
	wflSignalAt(env, 16*time.Second, "stage-transition", wflDone("billing", "B1", 3))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.True(t, first.completed && first.err == nil && first.rejected == nil, "the first defer was applied: %+v", first)
	assert.True(t, second.completed && second.err == nil && second.rejected == nil, "the second defer was applied: %+v", second)
	assert.Len(t, wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask), 2)
	assert.ElementsMatch(t,
		[]domain.NodeKey{"intake/A1", "warehouse/Pack", "billing/B1", "billing/B1", "intake/A1", "intake/A1", "billing/B1"},
		wflNodesCreated(fakes))
}

// A defer on a degraded instance whose fork is the plan's first step has no
// step to return to: the workflow refuses it, nothing is deferred or routed,
// and the failed branch stays parked.
func TestExecute_DeferOnADegradedInstanceWhoseForkIsTheFirstStepIsRefused(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/B1")
	collab := wflForkAfterIntake()
	collab.Plans[0].Execution.Steps = collab.Plans[0].Execution.Steps[1:]
	wflRegisterAll(env, fakes, collab)
	answer := &updateAnswer{}
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/B1", RecordVersion: 5})
	wflDeferTask(env, "defer-1", 8*time.Second, "billing/B1", answer)
	wflCancelAt(env, 10*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	refusal := answer.rejected
	if refusal == nil {
		refusal = answer.err
	}
	require.Error(t, refusal, "the defer was refused: %+v", answer)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, refusal, &appErr)
	assert.Equal(t, port.UpdateRefusedErrorType, appErr.Type())
	assert.Contains(t, appErr.Error(), "there is no completed step to go back to")
	assert.Empty(t, wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask))
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/Pack", "billing/B1", "billing/B1"}, wflNodesCreated(fakes),
		"the respawned branch's task stays open; nothing else ran")
}
