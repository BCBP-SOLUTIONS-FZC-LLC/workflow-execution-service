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

func wflIntakeThenFork() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "intake", Stages: wflStages("Intake")},
				{ID: "warehouse", Stages: wflStages("Pack")},
				{ID: "billing", Stages: wflStages("Charge")},
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

// A force-back inside a running parallel step records the route like any other
// force-back: one force-route record in the back direction that names the open
// branch tasks as abandoned and the re-entered department's first stage as the
// target, and the instance's current node moves to that stage.
func TestExecute_ForceBackInsideAParallelStepRecordsTheRouteAndMovesTheCurrentNode(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflIntakeThenFork())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-7", RecordVersion: 9})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("intake", "Intake", 2))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "Charge", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, wflInstance, audits[0].InstanceID)
	assert.Equal(t, domain.ForceRouteDirectionBack, audits[0].Direction)
	assert.Equal(t, "intake/Intake", audits[0].TargetNodeID)
	assert.Equal(t, []domain.NodeKey{"billing/Charge", "warehouse/Pack"}, audits[0].OldNodeKeys)
	assert.Equal(t, "admin-7", audits[0].AdminUserID)
	assert.Equal(t, int64(9), audits[0].RecordVersion)
	intakeWrites := 0
	for _, keys := range wflNodeWrites(fakes) {
		if len(keys) == 1 && keys[0] == "intake/Intake" {
			intakeWrites++
		}
	}
	assert.Equal(t, 3, intakeWrites, "the first task, the redirect and the re-entered task each wrote the current node")
}

// When the force-route record of a force-back inside a parallel step is
// refused, the run fails with that error and the department before the fork is
// not re-entered.
func TestExecute_ForceBackInsideAParallelStepWhoseAuditWriteIsRefusedFailsTheRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, _ any) error {
		if name == port.ActivityRecordForceRoute {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflIntakeThenFork())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-7", RecordVersion: 9})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityRecordForceRoute)
	assert.ElementsMatch(t, []domain.NodeKey{"intake/Intake", "billing/Charge", "warehouse/Pack"}, wflNodesCreated(fakes),
		"intake was not created a second time")
}

// A force-back inside a parallel step that is the first step has no completed
// step to return to: it is dropped with nothing changed, so no branch is left
// marked as paused, nothing is recorded and the branches finish normally.
func TestExecute_ForceBackInsideAParallelStepWithNothingBeforeItChangesNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := wflIntakeThenFork()
	collab.Plans[0].Execution.Steps = collab.Plans[0].Execution.Steps[1:]
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-7", RecordVersion: 9})
	var queried wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 8*time.Second, &queried)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "Charge", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Empty(t, queried.SavedSiblingBranches, "no branch was held back")
	assert.Zero(t, fakes.attemptsOf(port.ActivityRecordForceRoute))
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/Pack", "billing/Charge"}, wflNodesCreated(fakes))
}
