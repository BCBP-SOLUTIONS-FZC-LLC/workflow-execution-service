package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// dfkNested is intake, then a fork of branch a and a branch that runs mid and
// then an inner fork of b1 and b2.
func dfkNested() *dsl.CompiledCollaboration {
	seq := func(dept string) []dsl.ExecutionStep { return []dsl.ExecutionStep{{Sequential: []string{dept}}} }
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "intake", Stages: wflStages("I1")},
				{ID: "a", Stages: wflStages("A1")},
				{ID: "mid", Stages: wflStages("M1")},
				{ID: "b1", Stages: wflStages("T1")},
				{ID: "b2", Stages: wflStages("T2")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"intake"}},
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "a", Steps: seq("a")},
					{DeptID: "inner", Steps: []dsl.ExecutionStep{
						{Sequential: []string{"mid"}},
						{Parallel: []dsl.ParallelBranch{
							{DeptID: "b1", Steps: seq("b1")},
							{DeptID: "b2", Steps: seq("b2")},
						}},
					}},
				}},
			}},
		}},
	}
}

func dfkDefer(env *testsuite.TestWorkflowEnvironment, update string, delay time.Duration, dept, stage string, answer *updateAnswer) {
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("stage-defer:"+wflInstance, update, answer, deferWire{
			DeptID: dept, FromStage: stage, Reason: "need more info", UserID: "user-1", RecordVersion: 1,
			TaskID: dept + "/" + stage, AssignmentID: "assign-1",
		})
	}, delay)
}

func dfkAssertApplied(t *testing.T, answer *updateAnswer) {
	t.Helper()
	assert.True(t, answer.completed && answer.err == nil && answer.rejected == nil, "the defer was applied: %+v", answer)
}

func dfkVisitsOf(fakes *wflFakes, node domain.NodeKey) []int64 {
	var visits []int64
	for _, in := range wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask) {
		if in.NodeKey == node {
			visits = append(visits, in.VisitCount)
		}
	}
	return visits
}

// A defer of a task in a fork that is still running routes like a force-back:
// the sibling branch saves its position (its open task stays open), the step
// before the fork runs again, the deferring branch's department creates a new
// task, and the sibling then resumes where it was instead of starting over.
func TestExecute_DeferInARunningForkRerunsTheStepBeforeTheForkAndTheSiblingResumes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	answer := &updateAnswer{}
	var rerunOpen, afterRerun wfengine.WorkflowStatusQuery
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", answer)
	wflQueryStatusAt(t, env, 5*time.Second, &rerunOpen)
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflQueryStatusAt(t, env, 6500*time.Millisecond, &afterRerun)
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, answer)
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, wflTenant, deferred[0].TenantID)
	assert.Equal(t, "billing/B1", deferred[0].TaskID)
	assert.Equal(t, "intake/A1", deferred[0].DeferredToNodeKey)
	assert.ElementsMatch(t,
		[]domain.NodeKey{"intake/A1", "warehouse/Pack", "billing/B1", "intake/A1", "billing/B1"},
		wflNodesCreated(fakes), "the sibling's task was not created again")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/A1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "warehouse/Pack"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "billing/B1"))
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "warehouse/Pack"), "the sibling's task was never superseded")
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "billing/B1"),
		"the deferred task is closed by the defer itself, so only its replacement is written")
	assert.Equal(t, []domain.NodeKey{"intake/A1", "warehouse/Pack"}, rerunOpen.ActiveTasks, "open tasks are listed in name order")
	assert.ElementsMatch(t, []string{"warehouse", "billing"}, rerunOpen.SavedSiblingBranches, "both branches were saved")
	assert.Empty(t, afterRerun.SavedSiblingBranches, "nothing is saved once the rerun has ended, even for a branch with no waiting stage")
	assert.Empty(t, wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute), "a defer is not an admin route")
}

// Deferring the task the rerun itself created runs that step again at once.
func TestExecute_DeferOfTheRerunTaskBeforeAForkRunsTheStepAgain(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	first, second := &updateAnswer{}, &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", first)
	dfkDefer(env, "defer-2", 6*time.Second, "intake", "A1", second)
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("intake", "A1", 3))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, first)
	dfkAssertApplied(t, second)
	assert.Equal(t, []int64{1, 2, 3}, dfkVisitsOf(fakes, "intake/A1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "warehouse/Pack"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "billing/B1"), "the deferring branch resumed only after the last rerun")
}

// A cancel that arrives while the step before the fork runs again ends the
// instance, and the sibling's task was left alone by the defer.
func TestExecute_CancelWhileTheStepBeforeAForkRunsAgainTerminatesTheInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	var siblingBeforeCancel []domain.TaskStatus
	env.RegisterDelayedCallback(func() {
		siblingBeforeCancel = wflTaskStatusesOf(fakes, "warehouse/Pack")
	}, 5*time.Second)
	wflCancelAt(env, 6*time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	// Read before the cancel: the cancel itself may close the sibling's task.
	assert.Empty(t, siblingBeforeCancel, "the defer did not supersede the sibling's task")
}

// A fork that is the plan's first step has no step to return to: the defer is
// refused, nothing is deferred, and both branches keep running.
func TestExecute_DeferInAForkThatIsTheFirstStepIsRefusedAndTheBranchesKeepRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := wflForkAfterIntake()
	collab.Plans[0].Execution.Steps = collab.Plans[0].Execution.Steps[1:]
	wflRegisterAll(env, fakes, collab)
	answer := &updateAnswer{}
	dfkDefer(env, "defer-1", 3*time.Second, "billing", "B1", answer)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("billing", "B1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	refusal := answer.rejected
	if refusal == nil {
		refusal = answer.err
	}
	require.Error(t, refusal, "the defer was refused: %+v", answer)
	assert.Contains(t, refusal.Error(), "there is no completed step to go back to")
	assert.Empty(t, wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask))
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/Pack", "billing/B1"}, wflNodesCreated(fakes), "no rerun, no second creation")
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "billing/B1"))
}

// A second defer that arrives while the step before the fork already runs
// again does not start another rerun: the second branch's task is closed too,
// and both deferring branches create their new task after the one rerun.
func TestExecute_ASecondDeferWhileTheStepBeforeTheForkRunsAgainJoinsTheSameRerun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	first, second := &updateAnswer{}, &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", first)
	dfkDefer(env, "defer-2", 5*time.Second, "warehouse", "Pack", second)
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("warehouse", "Pack", 2))
	wflSignalAt(env, 11*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, first)
	dfkAssertApplied(t, second)
	assert.Len(t, wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask), 2)
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/A1"), "one rerun for both defers")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "warehouse/Pack"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "billing/B1"))
	for _, node := range []string{"warehouse/Pack", "billing/B1"} {
		assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, node))
	}
}

// An admin force-back that arrives while the step before the fork already runs
// again changes nothing: the same return is already under way, so it is not
// recorded and the step does not run a third time.
func TestExecute_ForceBackWhileTheStepBeforeAForkRunsAgainIsDropped(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-5", RecordVersion: 7})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/A1"))
	assert.Empty(t, wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute))
}

// A force-forward to a department of the fork that arrives while the step
// before the fork runs again is acted on at once: the target branch is forced
// forward, and the deferring branch still creates its new task afterwards.
func TestExecute_ForceForwardWhileTheStepBeforeAForkRunsAgainIsActedOn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{
		AdminUserID: "admin-5", TargetDeptID: "warehouse", TargetNodeKey: "warehouse/Pack", RecordVersion: 7,
	})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, domain.ForceRouteDirectionForward, audits[0].Direction)
	assert.Equal(t, "warehouse/Pack", audits[0].TargetNodeID)
	assert.Equal(t, "admin-5", audits[0].AdminUserID)
	assert.Equal(t, []domain.NodeKey{"warehouse/Pack"}, audits[0].OldNodeKeys)
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "warehouse/Pack"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "billing/B1"))
}

// A defer of a task in the outer fork's branch returns to the step before the
// outer fork: the inner fork's branches are saved and resume where they were.
func TestExecute_DeferInAnOuterBranchSavesTheInnerForkAndResumesItAfterTheRerun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, dfkNested())
	answer := &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "I1", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	dfkDefer(env, "defer-1", 5*time.Second, "a", "A1", answer)
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("intake", "I1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("b1", "T1", 1))
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("b2", "T2", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("a", "A1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, answer)
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, "intake/I1", deferred[0].DeferredToNodeKey, "the step before the outer fork, not before the inner one")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/I1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "a/A1"))
	for _, node := range []domain.NodeKey{"mid/M1", "b1/T1", "b2/T2"} {
		assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, node), "%s was not run again", node)
		assert.NotContains(t, wflTaskStatusesOf(fakes, string(node)), domain.TaskStatusSuperseded)
	}
}

// With an inner fork parked, a defer in the outer fork's other branch still
// returns to the step before the outer fork; the inner park stays as it is and
// is resolved by its own force-back afterwards.
func TestExecute_DeferInAnOuterBranchWhileAnInnerForkIsParkedLeavesThePark(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "b1/T1")
	wflRegisterAll(env, fakes, dfkNested())
	answer := &updateAnswer{}
	var rerunOpen wfengine.WorkflowStatusQuery
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "I1", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("b2", "T2", 1))
	dfkDefer(env, "defer-1", 5*time.Second, "a", "A1", answer)
	wflQueryStatusAt(t, env, 6*time.Second, &rerunOpen)
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("intake", "I1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("a", "A1", 2))
	wflSignalAt(env, 9*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "b1/T1", RecordVersion: 5})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("b1", "T1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, answer)
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, "intake/I1", deferred[0].DeferredToNodeKey)
	assert.Equal(t, domain.InstanceStatusDegraded, rerunOpen.Status, "the inner park was not closed by the outer defer")
	assert.Contains(t, rerunOpen.ActiveTasks, domain.NodeKey("intake/I1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/I1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "a/A1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "mid/M1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "b2/T2"), "the inner fork's finished branch was not run again")
}

// A defer of a task in an inner fork's branch returns to the step before the
// inner fork only: its other branch is saved and resumes, and the outer fork's
// other branch is left alone.
func TestExecute_DeferInAnInnerBranchRerunsTheStepBeforeTheInnerForkOnly(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, dfkNested())
	answer := &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "I1", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	dfkDefer(env, "defer-1", 5*time.Second, "b1", "T1", answer)
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("mid", "M1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("a", "A1", 1))
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("b2", "T2", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("b1", "T1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, answer)
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, "mid/M1", deferred[0].DeferredToNodeKey, "the step before the inner fork")
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "intake/I1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "a/A1"), "the outer fork's other branch was not touched")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "mid/M1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "b1/T1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "b2/T2"), "the inner sibling resumed instead of starting over")
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "b2/T2"))
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "a/A1"))
}

// A defer in a running fork closes a task the deferred stage's non-interrupting
// timer path opened beside it, and leaves the sibling branch's task alone.
func TestExecute_DeferInARunningForkSupersedesATaskItsTimerPathOpened(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := wflForkAfterIntake()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "esc", Stages: wflStages("E1")})
	plan.Departments[2].Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "3s", TargetDept: "esc"}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 7*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("billing", "B1", 2))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusSuperseded}, wflTaskStatusesOf(fakes, "esc/E1"), "the timer path's task was closed once")
	assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "warehouse/Pack"))
}

// An inner fork's rerun and an outer fork's rerun that overlap on the inner
// branches' departments both hold them: they are released only when the last
// rerun ends, and every branch then resumes.
func TestExecute_OverlappingInnerAndOuterDefersReleaseTheSharedBranchesAfterBothReruns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, dfkNested())
	inner, outer := &updateAnswer{}, &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "I1", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	dfkDefer(env, "defer-1", 5*time.Second, "b1", "T1", inner)
	dfkDefer(env, "defer-2", 6*time.Second, "a", "A1", outer)
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("mid", "M1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("intake", "I1", 2))
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("b2", "T2", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("a", "A1", 2))
	wflSignalAt(env, 11*time.Second, "stage-transition", wflDone("b1", "T1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, inner)
	dfkAssertApplied(t, outer)
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/I1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "mid/M1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "a/A1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "b1/T1"))
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "b2/T2"))
}

// A sibling that finishes its task while the step before the fork reruns is
// saved at its next step: that stage is created only after the rerun ends.
func TestExecute_ASiblingThatFinishesDuringTheRerunWaitsAtItsNextStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := wflForkAfterIntake()
	collab.Plans[0].Departments[1].Stages = wflStages("Pack", "Ship")
	wflRegisterAll(env, fakes, collab)
	var during, after wfengine.WorkflowStatusQuery
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflQueryStatusAt(t, env, 5500*time.Millisecond, &during)
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflQueryStatusAt(t, env, 6500*time.Millisecond, &after)
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("warehouse", "Ship", 1))
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"intake/A1"}, during.ActiveTasks, "the finished sibling has not started its next stage")
	assert.Equal(t, []string{"billing", "warehouse"}, during.SavedSiblingBranches, "saved branches are listed in name order")
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/Ship", "billing/B1"}, after.ActiveTasks, "both branches resumed once the rerun ended")
	assert.Empty(t, after.SavedSiblingBranches)
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "warehouse/Pack"))
}

// A defer applied while the stage is still being set up (its task is being
// created) is not lost: the stage ends as soon as it starts waiting.
func TestExecute_ADeferThatLandsBeforeTheStageIsWaitingStillEndsTheStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	var mu sync.Mutex
	var created []domain.NodeKey
	env.OnActivity(port.ActivityCreateTask, mock.Anything, mock.Anything).After(3 * time.Second).Return(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			created = append(created, in.NodeKey)
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		})
	answer := &updateAnswer{}
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 5*time.Second, "billing", "B1", answer)
	wflSignalAt(env, 9*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 13*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, answer)
	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []domain.NodeKey{"intake/A1", "warehouse/Pack", "billing/B1", "intake/A1", "billing/B1"}, created)
}

// dfkCallWithFork is a call whose body is a fork of p1 and p2 and whose timer
// path interrupts it after three seconds into esc; the step after it is after.
func dfkCallWithFork() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "p1", Stages: wflStages("P1")},
				{ID: "p2", Stages: wflStages("P2")},
				{ID: "esc", Stages: wflStages("E1")},
				{ID: "after", Stages: wflStages("A1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "Call", Name: "call",
					Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
						{DeptID: "p1", Steps: []dsl.ExecutionStep{{Sequential: []string{"p1"}}}},
						{DeptID: "p2", Steps: []dsl.ExecutionStep{{Sequential: []string{"p2"}}}},
					}}}},
					TimerPaths: []dsl.TimerPath{{Duration: "3s", Interrupting: true, TargetDept: "esc"}},
				}},
				{Sequential: []string{"after"}},
			}},
		}},
	}
}

// A fork inside a call whose interrupting path cancelled it leaves no state
// behind: an admin cancel sent afterwards still reaches the main run.
func TestExecute_ACancelledCallWithAForkInsideLeavesTheInstanceAbleToHandleAdminSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := dfkCallWithFork()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflCancelAt(env, 60*time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "after/A1"), "the run went on after the call")
}

// When the step before the fork also appears inside a branch, the rerun is not
// held back by its own pause: the department is not paused for the return.
func TestExecute_DeferWhenTheStepBeforeTheForkAlsoRunsInABranchStillReruns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := wflForkAfterIntake()
	collab.Plans[0].Execution.Steps[1].Parallel[1].Steps = []dsl.ExecutionStep{{Sequential: []string{"billing", "intake"}}}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("billing", "B1", 2))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("intake", "A1", 3))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []int64{1, 2, 3}, dfkVisitsOf(fakes, "intake/A1"), "the rerun, then the branch's own run of the department")
}

// A deferred task in a department with several stages restarts the branch at
// that stage after the rerun, not at the department's first stage.
func TestExecute_DeferOfALaterStageRestartsTheBranchAtThatStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := wflForkAfterIntake()
	collab.Plans[0].Departments[2].Stages = wflStages("B1", "B2")
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("billing", "B1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B2", &updateAnswer{})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("billing", "B2", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "billing/B1"), "the branch did not start over")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "billing/B2"))
}

// A park inside a call whose interrupting path cancelled it is closed: the
// instance status goes back to RUNNING and an admin cancel afterwards is not
// swallowed by the dead park.
func TestExecute_ACancelledCallWithAParkedForkInsideLeavesNoParkBehind(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := dfkCallWithFork()
	wflFailTaskOnce(fakes, "p1/P1")
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("p2", "P2", 1))
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflCancelAt(env, 60*time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusRunning}, wflStatuses(fakes),
		"the abandoned park gave the status back")
}

// When the status restore after an abandoned park is refused, the instance
// still counts as running: an admin pause is accepted, and the restore is tried
// again at the next step result.
func TestExecute_RefusedStatusRestoreAfterAnAbandonedParkDoesNotBlockLaterAdminSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	createRefused, restoreRefused := false, false
	fakes.failFor = func(name string, in any) error {
		switch v := in.(type) {
		case port.CreateTaskInput:
			if v.NodeKey == "p1/P1" && !createRefused {
				createRefused = true
				return wflRefuse(name)
			}
		case port.UpdateInstanceStatusInput:
			if v.Status == domain.InstanceStatusRunning && !restoreRefused {
				restoreRefused = true
				return wflRefuse(name)
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, dfkCallWithFork())
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("p2", "P2", 1))
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 10*time.Second, "instance-pause", wflAdminPause("admin"))
	wflSignalAt(env, 12*time.Second, "instance-resume", adminSignalWire{AdminUserID: "admin-1", Initiator: "admin", RecordVersion: 3})
	wflSignalAt(env, 14*time.Second, "stage-transition", wflDone("after", "A1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityPauseInstance), "the pause was accepted")
	assert.Equal(t, []domain.InstanceStatus{
		domain.InstanceStatusDegraded, domain.InstanceStatusRunning, domain.InstanceStatusRunning, domain.InstanceStatusCompleted,
	}, wflStatuses(fakes), "the refused RUNNING write was tried again at the next step result")
}

// A park whose call is cancelled while a defer's return to the step before the
// fork is still being written is closed like any abandoned park: the instance
// status goes back to RUNNING instead of staying DEGRADED.
func TestExecute_ParkAbandonedWhileADeferIsBeingRoutedGivesTheStatusBack(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := dfkCallWithFork()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "mid", Stages: wflStages("M1", "M2")})
	plan.Execution.Steps = append([]dsl.ExecutionStep{{Sequential: []string{"mid"}}}, plan.Execution.Steps...)
	plan.Execution.Steps[1].SubWorkflow.TimerPaths[0].Duration = "6s"
	wflFailTaskOnce(fakes, "p1/P1")
	wflRegisterAll(env, fakes, collab)
	env.OnActivity(port.ActivityUpdateInstanceNodes, mock.Anything, mock.MatchedBy(func(in port.UpdateInstanceNodesInput) bool {
		return len(in.NodeKeys) == 1 && in.NodeKeys[0] == "mid/M1"
	})).After(4 * time.Second).Return(nil)
	env.OnActivity(port.ActivityUpdateInstanceNodes, mock.Anything, mock.Anything).Return(nil)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("mid", "M2", 1))
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("p2", "P2", 1))
	wflSignalAt(env, 8*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "p1/P1", RecordVersion: 5})
	dfkDefer(env, "defer-1", 10*time.Second, "p1", "P1", &updateAnswer{})
	wflSignalAt(env, 15*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 17*time.Second, "stage-transition", wflDone("after", "A1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusRunning, domain.InstanceStatusCompleted},
		wflStatuses(fakes), "the abandoned park gave the status back")
}
