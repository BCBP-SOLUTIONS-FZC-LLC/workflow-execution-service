package workflow_test

import (
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

// w28aForkAfterIntakeEndingByTimer is the fork after one intake step whose
// task ends its path, with no target, when an hour has passed without an answer.
func w28aForkAfterIntakeEndingByTimer() *dsl.CompiledCollaboration {
	collab := wflForkAfterIntake()
	collab.Plans[0].Departments[0].Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, Terminates: true}
	return collab
}

// When the rerun of the step before the fork ends its path (a boundary with no
// target fired), the saved branches resume exactly as after a clean end.
func TestExecute_RerunOfTheStepBeforeAForkThatEndsItsPath_LetsTheSavedBranchesResume(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w28aForkAfterIntakeEndingByTimer())
	answer := &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", answer)
	var saved wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 5*time.Second, &saved)
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	var resumed wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 2*time.Hour, &resumed)
	wflSignalAt(env, 3*time.Hour, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	dfkAssertApplied(t, answer)
	assert.ElementsMatch(t, []string{"warehouse", "billing"}, saved.SavedSiblingBranches, "both branches were held for the rerun")
	assert.Empty(t, resumed.SavedSiblingBranches, "the path end released them")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/A1"), "the step ran once more")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "billing/B1"), "the deferring branch created its new task after the rerun")
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "warehouse/Pack"))
	assert.Zero(t, fakes.attemptsOf(port.ActivityRecordForceRoute), "a defer is not an admin route")
}

// A force-back held during the rerun is replayed when the rerun ends its path
// too, not only when it ends cleanly: it is recorded once and the step runs again.
func TestExecute_RerunOfTheStepBeforeAForkThatEndsItsPath_ReplaysAHeldForceBack(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w28aForkAfterIntakeEndingByTimer())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-5", RecordVersion: 7})
	wflSignalAt(env, 2*time.Hour, "stage-transition", wflDone("intake", "A1", 3))
	wflSignalAt(env, 2*time.Hour+time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 2*time.Hour+2*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []int64{1, 2, 3}, dfkVisitsOf(fakes, "intake/A1"), "the first run, the defer's rerun, the replayed force-back")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, "admin-5", audits[0].AdminUserID)
	assert.Equal(t, int64(7), audits[0].RecordVersion)
	assert.Equal(t, domain.ForceRouteDirectionBack, audits[0].Direction)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, wflInstance, audits[0].InstanceID)
}

// A rerun of the step before the fork that fails for any other reason fails the
// run with that error.
func TestExecute_RerunOfTheStepBeforeAForkThatFails_FailsTheRunWithThatError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	failed := wflDone("intake", "A1", 2)
	failed.Failed, failed.Reason = true, "intake_rejected"
	wflSignalAt(env, 6*time.Second, "stage-transition", failed)

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `stage "intake/A1" failed: intake_rejected`)
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "intake/A1"))
	assert.Contains(t, wflStatuses(fakes), domain.InstanceStatusFailed)
	assert.NotContains(t, wflStatuses(fakes), domain.InstanceStatusCompleted)
}

// A defer in a running fork records no route; the only write that can be refused
// is the current-node write for the step before the fork. Refusing it fails the
// run with that error, and nothing is rerun.
func TestExecute_DeferInARunningForkWhoseNodeWriteIsRefusedFailsTheRunAndReRunsNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	intakeWrites := 0
	fakes.failFor = func(name string, in any) error {
		if v, ok := in.(port.UpdateInstanceNodesInput); ok && len(v.NodeKeys) == 1 && v.NodeKeys[0] == "intake/A1" {
			intakeWrites++
			if intakeWrites == 2 {
				return wflRefuse(name)
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityUpdateInstanceNodes)
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "intake/A1"), "the step before the fork was not rerun")
	assert.Zero(t, fakes.attemptsOf(port.ActivityRecordForceRoute), "a defer records no route")
	assert.ElementsMatch(t, []domain.NodeKey{"intake/A1", "warehouse/Pack", "billing/B1"}, wflNodesCreated(fakes), "no task was created again")
}

// w28aMidThenCallWithFork runs mid (two stages), then a call whose body is a fork
// of p1 and p2 and whose timer path interrupts it after six seconds into esc.
func w28aMidThenCallWithFork() *dsl.CompiledCollaboration {
	collab := dfkCallWithFork()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "mid", Stages: wflStages("M1", "M2")})
	plan.Execution.Steps = append([]dsl.ExecutionStep{{Sequential: []string{"mid"}}}, plan.Execution.Steps...)
	plan.Execution.Steps[1].SubWorkflow.TimerPaths[0].Duration = "6s"
	return collab
}

// A fork that is itself abandoned (the call around it was interrupted) while its
// rerun runs drops the rerun's result and releases the branches it held.
func TestExecute_ForkAbandonedWhileItsRerunRuns_ReleasesItsHoldsAndTheRunGoesOn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w28aMidThenCallWithFork())
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("mid", "M2", 1))
	// The call starts at 2s; its timer path interrupts it at 8s.
	dfkDefer(env, "defer-1", 4*time.Second, "p1", "P1", &updateAnswer{})
	var holding, released wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 5*time.Second, &holding)
	wflQueryStatusAt(t, env, 9*time.Second, &released)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("after", "A1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.ElementsMatch(t, []string{"p1", "p2"}, holding.SavedSiblingBranches, "the rerun held the branches")
	assert.Empty(t, released.SavedSiblingBranches, "the abandoned fork released them")
	assert.Equal(t, []int64{1, 2}, dfkVisitsOf(fakes, "mid/M1"), "the rerun started")
	assert.Contains(t, w28aSupersededOf(fakes), "mid/M1", "the open task of the dropped rerun is superseded")
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "esc/E1"), "the interrupting path ran")
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "after/A1"), "the run went on after the call")
}

// A run abandoned while the current-node write for a defer's return is in flight
// ends the stage as abandoned: the interrupting path runs and the run goes on,
// instead of failing with the write's outcome.
func TestExecute_CallInterruptedWhileADefersNodeWriteIsInFlight_EndsTheStageAsAbandoned(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w28aMidThenCallWithFork())
	env.OnActivity(port.ActivityUpdateInstanceNodes, mock.Anything, mock.MatchedBy(func(in port.UpdateInstanceNodesInput) bool {
		return len(in.NodeKeys) == 1 && in.NodeKeys[0] == "mid/M1"
	})).After(6 * time.Second).Return(nil)
	env.OnActivity(port.ActivityUpdateInstanceNodes, mock.Anything, mock.Anything).Return(nil)
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("mid", "M1", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("mid", "M2", 1))
	// The call starts at 6s, once mid's first write returns; its timer path interrupts it at 12s.
	dfkDefer(env, "defer-1", 8*time.Second, "p1", "P1", &updateAnswer{})
	wflSignalAt(env, 16*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 18*time.Second, "stage-transition", wflDone("after", "A1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "mid/M1"), "the write was cut short, so the step before the fork never reran")
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "esc/E1"), "the interrupting path ran")
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "after/A1"), "the run went on after the call")
	assert.Zero(t, fakes.attemptsOf(port.ActivityRecordForceRoute), "a defer records no route")
	assert.Contains(t, w28aSupersededOf(fakes), "p2/P2", "the sibling's open task was abandoned with the call")
}
