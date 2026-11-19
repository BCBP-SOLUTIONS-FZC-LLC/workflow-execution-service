package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestExecute_StatusQueryWithTwoPendingStages_ListsBothNodeKeysAsCurrentAndActive(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, parallelCollaboration())
	var open wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 2*time.Second, &open)
	wflSignalAt(env, 4*time.Second, "stage-transition", w23bWire("warehouse", 1, 1))
	var oneLeft wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 5*time.Second, &oneLeft)
	wflSignalAt(env, 6*time.Second, "stage-transition", w23bWire("billing", 1, 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusRunning, open.Status)
	assert.Equal(t, []domain.NodeKey{"billing/prep", "warehouse/prep"}, open.CurrentNodeKeys)
	assert.Equal(t, open.CurrentNodeKeys, open.ActiveTasks, "active tasks list the same pending nodes")
	assert.Empty(t, open.SavedSiblingBranches)
	assert.Empty(t, open.UnassignedTasks)
	assert.Equal(t, []domain.NodeKey{"billing/prep"}, oneLeft.CurrentNodeKeys)
	assert.Equal(t, oneLeft.CurrentNodeKeys, oneLeft.ActiveTasks)

	val, qerr := env.QueryWorkflow(wfengine.QueryGetWorkflowStatus)
	require.NoError(t, qerr)
	var done wfengine.WorkflowStatusQuery
	require.NoError(t, val.Get(&done))
	assert.Equal(t, domain.InstanceStatusCompleted, done.Status)
	assert.Empty(t, done.CurrentNodeKeys, "nothing is pending after completion")
	assert.Empty(t, done.ActiveTasks)
	assert.Empty(t, done.SavedSiblingBranches)
}

func TestExecute_StatusQueryDuringADeferRerun_ListsTheSavedSiblingBranches(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	var during wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 5*time.Second, &during)
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	var after wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 8*time.Second, &after)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusRunning, during.Status)
	assert.Equal(t, []domain.NodeKey{"intake/A1", "warehouse/Pack"}, during.CurrentNodeKeys, "the rerun of the step before the fork is pending, and the saved branch keeps its task")
	assert.Equal(t, during.CurrentNodeKeys, during.ActiveTasks)
	assert.Equal(t, []string{"billing", "warehouse"}, during.SavedSiblingBranches, "the fork's branches wait at the pause gate during the rerun")
	assert.Empty(t, after.SavedSiblingBranches, "the gate is open again once the rerun ended")
	assert.Equal(t, []domain.NodeKey{"billing/B1", "warehouse/Pack"}, after.CurrentNodeKeys)
}

func TestExecute_StatusQueryAcrossPauseResumeAndDegraded_ReportsEachStatusWithItsPendingKeys(t *testing.T) {
	t.Run("pause then resume", func(t *testing.T) {
		env := w23bNewEnv()
		fakes := wflNewFakes()
		wflRegisterAll(env, fakes, w23bOneTask())
		var running, paused, resumed wfengine.WorkflowStatusQuery
		wflQueryStatusAt(t, env, 2*time.Second, &running)
		wflSignalAt(env, 3*time.Second, "instance-pause", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 2})
		wflQueryStatusAt(t, env, 4*time.Second, &paused)
		wflSignalAt(env, 5*time.Second, "instance-resume", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 3})
		wflQueryStatusAt(t, env, 6*time.Second, &resumed)
		wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("sales", "n1", 1))

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status)
		assert.Equal(t, domain.InstanceStatusRunning, running.Status)
		assert.Equal(t, domain.InstanceStatusPaused, paused.Status)
		assert.Equal(t, []domain.NodeKey{"sales/n1"}, paused.CurrentNodeKeys, "the pending key is still listed while paused")
		assert.Equal(t, paused.CurrentNodeKeys, paused.ActiveTasks)
		assert.Equal(t, domain.InstanceStatusRunning, resumed.Status)
		assert.Equal(t, []domain.NodeKey{"sales/n1"}, resumed.CurrentNodeKeys)
	})

	t.Run("degraded park", func(t *testing.T) {
		env := w23bNewEnv()
		fakes := wflNewFakes()
		wflRefuseTaskFor(fakes, "billing/prep")
		wflRegisterAll(env, fakes, parallelCollaboration())
		wflSignalAt(env, 2*time.Second, "stage-transition", w23bWire("warehouse", 1, 1))
		var degraded wfengine.WorkflowStatusQuery
		wflQueryStatusAt(t, env, 6*time.Second, &degraded)
		wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 5})

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status)
		assert.Equal(t, domain.InstanceStatusDegraded, degraded.Status)
		assert.Empty(t, degraded.CurrentNodeKeys, "the failed branch has no pending task")
		writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
		require.NotEmpty(t, writes)
		assert.Equal(t, domain.InstanceStatusDegraded, writes[0].Status, "the park wrote the degraded status the query reports")
	})
}

// w23bTwoForks is two parallel steps in a row, so the bookkeeping of the first
// must be gone before the second opens.
func w23bTwoForks() *dsl.CompiledCollaboration {
	fork := func(a, b string) dsl.ExecutionStep {
		return dsl.ExecutionStep{Parallel: []dsl.ParallelBranch{
			{DeptID: a, Steps: []dsl.ExecutionStep{{Sequential: []string{a}}}},
			{DeptID: b, Steps: []dsl.ExecutionStep{{Sequential: []string{b}}}},
		}}
	}
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "a", Stages: wflStages("A")}, {ID: "b", Stages: wflStages("B")},
				{ID: "c", Stages: wflStages("C")}, {ID: "d", Stages: wflStages("D")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{fork("a", "b"), fork("c", "d")}},
		}},
	}
}

func TestExecute_TwoParallelStepsInARow_EachForkSeesOnlyItsOwnBranchesAndTheRunCompletes(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w23bTwoForks())
	wflSignalAt(env, 2*time.Second, "stage-transition", stageTransitionWire{DeptID: "a", NodeID: "A", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})
	wflSignalAt(env, 2*time.Second, "stage-transition", stageTransitionWire{DeptID: "b", NodeID: "B", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})
	var second wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 5*time.Second, &second)
	wflSignalAt(env, 6*time.Second, "stage-transition", stageTransitionWire{DeptID: "c", NodeID: "C", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})
	wflSignalAt(env, 6*time.Second, "stage-transition", stageTransitionWire{DeptID: "d", NodeID: "D", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"c/C", "d/D"}, second.CurrentNodeKeys, "only the second fork's branches are pending")
	assert.Empty(t, second.SavedSiblingBranches)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusCompleted}, wflStatuses(fakes))
}
