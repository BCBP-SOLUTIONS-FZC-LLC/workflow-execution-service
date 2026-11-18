package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func fxbForkThenTail() *dsl.CompiledCollaboration {
	collab := wflForkAfterIntake()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "tail", Stages: wflStages("Tail")})
	plan.Execution.Steps = append(plan.Execution.Steps, dsl.ExecutionStep{Sequential: []string{"tail"}})
	return collab
}

func fxbForceForward(env *testsuite.TestWorkflowEnvironment, delay time.Duration, target string, recordVersion int64) {
	wflSignalAt(env, delay, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: target, RecordVersion: recordVersion})
}

// Force-routing every branch of a running top-level fork forward ends the fork
// and leaves no fork state behind: a defer sent afterwards goes back to the
// step before the fork and runs it again, and a force-forward after that is an
// ordinary top-level route.
func TestExecute_ForceRouteOutOfARunningTopLevelForkLeavesNoForkState(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, fxbForkThenTail())
	var afterRoute, afterDefer, afterForward wfengine.WorkflowStatusQuery
	answer := &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	fxbForceForward(env, 4*time.Second, "warehouse/Pack", 2)
	fxbForceForward(env, 4500*time.Millisecond, "billing/B1", 3)
	wflQueryStatusAt(t, env, 5*time.Second, &afterRoute)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("stage-defer:"+wflInstance, "defer-1", answer, deferWire{
			DeptID: "tail", FromStage: "Tail", Reason: "need more info", UserID: "user-1", RecordVersion: 1,
			TaskID: "tail/Tail", AssignmentID: "assign-1",
		})
	}, 6*time.Second)
	wflQueryStatusAt(t, env, 7*time.Second, &afterDefer)
	fxbForceForward(env, 8*time.Second, "tail/Tail", 4)
	wflQueryStatusAt(t, env, 9*time.Second, &afterForward)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("tail", "Tail", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"tail/Tail"}, afterRoute.ActiveTasks, "the fork ended and the step after it opened")
	assert.Empty(t, afterRoute.SavedSiblingBranches)
	dfkAssertApplied(t, answer)
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, "tail/Tail", deferred[0].TaskID)
	assert.Equal(t, "intake/A1", deferred[0].DeferredToNodeKey, "the defer went to the step before the fork")
	assert.Equal(t, []domain.NodeKey{"intake/A1"}, afterDefer.ActiveTasks, "the defer reran the step before the fork, not a fork rerun")
	assert.Empty(t, afterDefer.SavedSiblingBranches, "no branch was saved: the fork is gone")
	assert.Equal(t, []domain.NodeKey{"tail/Tail"}, afterForward.ActiveTasks)
	assert.Empty(t, afterForward.SavedSiblingBranches)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 3)
	assert.Equal(t, []string{string(domain.ForceRouteDirectionForward)}, []string{routes[2].Direction})
	assert.Equal(t, []domain.NodeKey{"intake/A1"}, routes[2].OldNodeKeys, "the last route left the rerun task")
	assert.Equal(t, "tail/Tail", routes[2].TargetNodeID)
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/A1"))
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "tail/Tail"))
}
