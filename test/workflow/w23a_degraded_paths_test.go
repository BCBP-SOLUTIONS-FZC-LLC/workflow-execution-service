package workflow_test

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// Forwarding a parked failed branch records the route from the node the branch
// last completed to the node the admin named, writes that node as the current
// node once, and removes the branch from the failed list: the instance goes back
// to RUNNING with no failed branch.
func TestExecute_ForceForwardOfAParkedBranchWritesTheTargetAsTheCurrentNodeAndEmptiesTheFailedList(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/X2")
	wflRegisterAll(env, fakes, wflTwoDeptBranch())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("prev", "P1", 1))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("billing", "X1", 1))
	wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-5", TargetNodeKey: "billing/X2", RecordVersion: 3})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, []domain.NodeKey{"billing/X1"}, routes[0].OldNodeKeys, "the node the parked branch last completed")
	assert.Equal(t, "billing/X2", routes[0].TargetNodeID)
	redirects := 0
	for _, keys := range wflNodeWrites(fakes) {
		if slices.Equal(keys, []domain.NodeKey{"billing/X2"}) {
			redirects++
		}
	}
	assert.Equal(t, 1, redirects, "the target is written as the current node once: the failed task of X2 was never created")
	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	require.Len(t, writes, 3)
	assert.Equal(t, domain.InstanceStatusDegraded, writes[0].Status)
	require.Len(t, writes[0].FailedBranches, 1)
	assert.Equal(t, "billing/X1", writes[0].FailedBranches[0].LastNodeKey)
	assert.Equal(t, domain.InstanceStatusRunning, writes[1].Status)
	assert.Empty(t, writes[1].FailedBranches, "no failed branch remains")
	assert.Equal(t, domain.InstanceStatusCompleted, writes[2].Status)
}

// w23aSpanFork is a fork whose billing branch sends message "m" in its first
// stage and fails in its second, next to an ops branch of one task; after the
// fork a department waits for "m".
func w23aSpanFork() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "billing", Stages: []dsl.StageDef{
					{Type: "send_task", NodeID: "Send", Extras: map[string]string{"message": "m"}},
					{Type: "prep", NodeID: "Charge"},
				}},
				{ID: "ops", Stages: wflStages("Gate")},
				{ID: "after", Stages: []dsl.StageDef{
					{Type: "receive_task", NodeID: "Wait", Extras: map[string]string{"message": "m"}},
				}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
					{DeptID: "ops", Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
				}},
				{Sequential: []string{"after"}},
			}},
		}},
	}
}

// A force-back of a parked failed branch writes an audit record in the back
// direction at the stage the branch resumes at, and forgets the messages the
// branch's last completed node sent: the receive that follows waits for a new
// one. A force-forward of the same branch keeps them, so the receive takes the
// message the branch already sent and the instance completes.
func TestExecute_ForceBackOfAParkedBranchRecordsTheRouteAndResetsItsMessageSpanWhereForwardDoesNot(t *testing.T) {
	t.Run("force-back", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		fakes := wflNewFakes()
		wflFailTaskOnce(fakes, "billing/Charge")
		wflRegisterAll(env, fakes, w23aSpanFork())
		wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("ops", "Gate", 1))
		wflSignalAt(env, 4*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-8", TargetNodeKey: "billing/Charge", RecordVersion: 6})
		wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("billing", "Charge", 2))
		var completedWhileWaiting bool
		wflclock.After(env, func() { completedWhileWaiting = env.IsWorkflowCompleted() }, time.Minute)
		wflCancelAt(env, 2*time.Minute)

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.False(t, completedWhileWaiting, "the wait for the message the branch sent is still open")
		assert.Equal(t, domain.InstanceStatusTerminated, status)
		routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
		require.Len(t, routes, 1)
		assert.Equal(t, port.RecordForceRouteInput{
			InstanceID: wflInstance, TenantID: wflTenant, TargetNodeID: "billing/Charge",
			Direction: domain.ForceRouteDirectionBack, AdminUserID: "admin-8", RecordVersion: 6,
		}, routes[0])
		assert.ElementsMatch(t, []domain.NodeKey{"billing/Charge", "ops/Gate", "billing/Charge"}, wflNodesCreated(fakes), "the failed stage was created again")
	})

	t.Run("force-forward", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		fakes := wflNewFakes()
		wflFailTaskOnce(fakes, "billing/Charge")
		wflRegisterAll(env, fakes, w23aSpanFork())
		wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("ops", "Gate", 1))
		wflSignalAt(env, 4*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-8", TargetNodeKey: "billing/Charge", RecordVersion: 6})

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status)
	})
}

// w23aTwoParkFork is two inner forks, x and y, each with a branch that fails on
// its first task, so two parks open together. The failing departments carry
// distinct IAM ids so a stored failed-branch list can be told apart.
const (
	w23aX2IAMID = "018e1f2a-0000-7000-8000-0000000000d1"
	w23aY2IAMID = "018e1f2a-0000-7000-8000-0000000000e2"
)

func w23aTwoParkFork() *dsl.CompiledCollaboration {
	inner := func(id, left, right string) dsl.ParallelBranch {
		return dsl.ParallelBranch{DeptID: id, Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
			{DeptID: left, Steps: []dsl.ExecutionStep{{Sequential: []string{left}}}},
			{DeptID: right, Steps: []dsl.ExecutionStep{{Sequential: []string{right}}}},
		}}}}
	}
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "x1", Stages: wflStages("P")},
				{ID: "x2", IAMDepartmentID: w23aX2IAMID, Stages: wflStages("P")},
				{ID: "y1", Stages: wflStages("P")},
				{ID: "y2", IAMDepartmentID: w23aY2IAMID, Stages: wflStages("P")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				inner("x", "x1", "x2"), inner("y", "y1", "y2"),
			}}}},
		}},
	}
}

// When one of two open parks drains, the instance stays DEGRADED and the status
// is rewritten with only the branch still failed in the other park; RUNNING is
// written once, when the last park drains.
func TestExecute_AParkThatDrainsWhileAnotherIsOpenRewritesDegradedWithOnlyTheRemainingBranch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && (v.NodeKey == "x2/P" || v.NodeKey == "y2/P") {
			return wflRefuse("create " + string(v.NodeKey))
		}
		return nil
	}
	wflRegisterAll(env, fakes, w23aTwoParkFork())
	wflSignalsAt(env, 2*time.Second,
		wflSignal{"stage-transition", wflDone("x1", "P", 1)},
		wflSignal{"stage-transition", wflDone("y1", "P", 1)},
	)
	wflSignalAt(env, 6*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "y2/P", RecordVersion: 1})
	wflSignalAt(env, 10*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "x2/P", RecordVersion: 2})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	runningAt := slices.IndexFunc(writes, func(in port.UpdateInstanceStatusInput) bool { return in.Status == domain.InstanceStatusRunning })
	require.Greater(t, runningAt, 1, "RUNNING is written after the degraded writes")
	assert.Equal(t, -1, slices.IndexFunc(writes[runningAt+1:], func(in port.UpdateInstanceStatusInput) bool { return in.Status == domain.InstanceStatusRunning }),
		"RUNNING is written once")
	assert.Equal(t, domain.InstanceStatusCompleted, writes[len(writes)-1].Status)
	interim := writes[runningAt-1]
	assert.Equal(t, domain.InstanceStatusDegraded, interim.Status, "the instance is not set RUNNING while a park is open")
	assert.Equal(t, wflInstance, interim.InstanceID)
	assert.Equal(t, wflTenant, interim.TenantID)
	require.Len(t, interim.FailedBranches, 1)
	assert.Equal(t, uuid.MustParse(w23aX2IAMID), interim.FailedBranches[0].DepartmentID, "only the branch of the park still open")
	assert.Empty(t, writes[runningAt].FailedBranches)
	both := slices.IndexFunc(writes, func(in port.UpdateInstanceStatusInput) bool { return len(in.FailedBranches) == 2 })
	assert.NotEqual(t, -1, both, "both parks' branches were listed while both were open")
	assert.Less(t, both, runningAt-1)
}

// w23aForkWithIntakeBoundary is an intake department whose stage carries the
// given boundary timer, then a fork of warehouse and billing.
func w23aForkWithIntakeBoundary(timer *dsl.BoundaryTimer) *dsl.CompiledCollaboration {
	collab := wflIntakeThenFork()
	collab.Plans[0].Departments[0].Stages[0].BoundaryTimer = timer
	return collab
}

// While the step before a fork reruns after a force-back inside the fork, the
// fork loop keeps reading admin signals and keeps its branches held; how the
// rerun ends decides what happens next.
func TestExecute_TheForkLoopKeepsReadingAdminSignalsWhileTheStepBeforeItReruns(t *testing.T) {
	endsItsPath := &dsl.BoundaryTimer{Duration: "1h", Interrupting: true}
	forceBack := adminSignalWire{AdminUserID: "admin-7", RecordVersion: 9}

	t.Run("a path end from the rerun is ignored and the branches resume", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		fakes := wflNewFakes()
		wflRegisterAll(env, fakes, w23aForkWithIntakeBoundary(endsItsPath))
		wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
		wflSignalAt(env, 5*time.Second, "instance-force-back", forceBack)
		wflSignalsAt(env, 2*time.Hour,
			wflSignal{"stage-transition", wflDone("warehouse", "Pack", 1)},
			wflSignal{"stage-transition", wflDone("billing", "Charge", 1)},
		)

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status, "the rerun's timer ended its path, which only ends the rerun")
		assert.Equal(t, 2, w19bCountNode(wflNodesCreated(fakes), "intake/Intake"), "the step before the fork ran again")
		assert.Equal(t, []string{"intake/Intake"}, w19bSupersededTasks(fakes), "the rerun task was superseded by its own timer")
	})

	t.Run("another error from the rerun fails the instance", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		fakes := wflNewFakes()
		fakes.failFor = func(_ string, in any) error {
			if v, ok := in.(port.CreateTaskInput); ok && v.NodeKey == "intake/Intake" && v.VisitCount == 2 {
				return wflRefuse("rerun of intake")
			}
			return nil
		}
		wflRegisterAll(env, fakes, w23aForkWithIntakeBoundary(endsItsPath))
		wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
		wflSignalAt(env, 5*time.Second, "instance-force-back", forceBack)

		_, err := wflStart(t, env, fakes, "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "workflow: CreateTaskActivity: ")
		assert.Contains(t, err.Error(), "rerun of intake refused")
		assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
	})

	t.Run("a cancel that arrives during the rerun ends the instance", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		fakes := wflNewFakes()
		wflRegisterAll(env, fakes, wflIntakeThenFork())
		wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
		wflSignalAt(env, 5*time.Second, "instance-force-back", forceBack)
		wflCancelAt(env, 8*time.Second)

		status, err := wflStart(t, env, fakes, "")

		wflAssertCancelEndedTheInstance(t, fakes, status, err)
		assert.Equal(t, 2, w19bCountNode(wflNodesCreated(fakes), "intake/Intake"), "the rerun was running when the cancel came")
	})
}
