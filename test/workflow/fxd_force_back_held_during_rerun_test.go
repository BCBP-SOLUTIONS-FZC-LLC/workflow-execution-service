package workflow_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// Every force-back the admin was given a 202 for is carried out: those that
// arrive while the step before the fork already runs again are held, in
// order, and each one returns to that step once the rerun in front of it ends.
func TestExecute_ForceBacksArrivingDuringAForkRerunAreReplayedOneByOneAfterIt(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-5", RecordVersion: 7})
	wflSignalAt(env, 5500*time.Millisecond, "instance-force-back", adminSignalWire{AdminUserID: "admin-6", RecordVersion: 8})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 7*time.Second, "stage-transition", wflDone("intake", "A1", 3))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("intake", "A1", 4))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []int64{1, 2, 3, 4}, dfkVisitsOf(fakes, "intake/A1"), "the defer's rerun, then one rerun per held force-back")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 2, "a defer is not audited; each held force-back is, once")
	for i, admin := range []string{"admin-5", "admin-6"} {
		assert.Equal(t, admin, audits[i].AdminUserID)
		assert.Equal(t, domain.ForceRouteDirectionBack, audits[i].Direction)
		assert.Equal(t, "intake/A1", audits[i].TargetNodeID)
	}
	assert.Equal(t, []int64{1}, dfkVisitsOf(fakes, "warehouse/Pack"), "the saved branch resumes where it was")
}

// fxdForkWithTwoStageBranch is intake, then a fork of a warehouse branch whose
// department has two stages and a billing branch.
func fxdForkWithTwoStageBranch() *dsl.CompiledCollaboration {
	collab := wflForkAfterIntake()
	collab.Plans[0].Departments[1].Stages = wflStages("Pack", "Label")
	return collab
}

// The saved branches stay saved across a replayed force-back: a branch result
// that finishes while the first rerun runs waits at the pause gate, and the
// gate stays shut across the replay's audit write until the replayed rerun ends.
func TestExecute_ASavedBranchStaysSavedWhileAHeldForceBackIsReplayed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, fxdForkWithTwoStageBranch())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-5", RecordVersion: 7})
	wflSignalAt(env, 5500*time.Millisecond, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("intake", "A1", 2))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("intake", "A1", 3))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "Label", 1))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("billing", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	created := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	labelAt, thirdIntakeAt := -1, -1
	for i, in := range created {
		switch {
		case in.NodeKey == "warehouse/Label":
			labelAt = i
		case in.NodeKey == "intake/A1" && in.VisitCount == 3:
			thirdIntakeAt = i
		}
	}
	require.NotEqual(t, -1, labelAt, "the branch reaches its next stage in the end")
	require.NotEqual(t, -1, thirdIntakeAt, "the held force-back ran its rerun")
	assert.Less(t, thirdIntakeAt, labelAt, "the branch moved on only after the replayed rerun ended")
}

// fxdWarnings records the warnings of a test workflow environment.
type fxdWarnings struct {
	mu   sync.Mutex
	logs []fxdWarning
}

type fxdWarning struct {
	msg     string
	keyvals []any
}

func (w *fxdWarnings) Debug(string, ...any) {}
func (w *fxdWarnings) Info(string, ...any)  {}
func (w *fxdWarnings) Error(string, ...any) {}
func (w *fxdWarnings) Warn(msg string, keyvals ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logs = append(w.logs, fxdWarning{msg: msg, keyvals: keyvals})
}

func (w *fxdWarnings) withText(text string) []fxdWarning {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []fxdWarning
	for _, l := range w.logs {
		if strings.Contains(l.msg, text) {
			out = append(out, l)
		}
	}
	return out
}

// A force-back still held when the fork ends (here, by an instance cancel) is
// not carried out: the instance is ending, so there is no audit, but the drop is
// logged with how many were lost.
func TestExecute_HeldForceBacksLostToAnInstanceCancelAreLoggedWithTheirCount(t *testing.T) {
	warnings := &fxdWarnings{}
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(warnings)
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	dfkDefer(env, "defer-1", 4*time.Second, "billing", "B1", &updateAnswer{})
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-5", RecordVersion: 7})
	wflSignalAt(env, 5200*time.Millisecond, "instance-force-back", adminSignalWire{AdminUserID: "admin-6", RecordVersion: 8})
	wflSignalAt(env, 5500*time.Millisecond, "instance-cancel", adminSignalWire{AdminUserID: "admin-9", RecordVersion: 9})

	_, _ = wflStart(t, env, fakes, "")

	dropped := warnings.withText("held force-back")
	require.Len(t, dropped, 1)
	assert.Contains(t, dropped[0].keyvals, 2, "the count of held force-backs that were dropped")
	assert.Empty(t, wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute), "no audit: nothing was carried out")
}
