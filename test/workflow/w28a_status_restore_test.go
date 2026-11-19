package workflow_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// w28aOrder records the order in which activities run, as short labels.
type w28aOrder struct {
	mu     sync.Mutex
	events []string
}

func (o *w28aOrder) add(e string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *w28aOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.events...)
}

func (o *w28aOrder) indexOf(label string, nth int) int {
	seen := 0
	for i, e := range o.snapshot() {
		if e == label {
			seen++
			if seen == nth {
				return i
			}
		}
	}
	return -1
}

// w28aRefuseFirstRestoreRig fails the first task of p1 (so the fork inside the
// call parks), refuses the first RUNNING status write (the restore after the
// timer path abandoned the park) and records the order of what runs.
func w28aRefuseFirstRestoreRig() (*wflFakes, *w28aOrder) {
	fakes := wflNewFakes()
	order := &w28aOrder{}
	createRefused, restoreRefused := false, false
	fakes.failFor = func(name string, in any) error {
		switch v := in.(type) {
		case port.CreateTaskInput:
			if v.NodeKey == "p1/P1" && !createRefused {
				createRefused = true
				return wflRefuse(name)
			}
		case port.UpdateInstanceStatusInput:
			order.add("status:" + string(v.Status))
			if v.Status == domain.InstanceStatusRunning && !restoreRefused {
				restoreRefused = true
				return wflRefuse(name)
			}
		case port.CompleteAssignmentInput:
			order.add("complete:" + v.TaskID)
		case port.PauseInstanceInput:
			order.add("pause")
		case port.ResumeInstanceInput:
			order.add("resume")
		}
		return nil
	}
	return fakes, order
}

// The restore of the RUNNING status that an abandoned park could not write is
// tried again when the next step result arrives, before that result is delivered.
func TestExecute_RefusedStatusRestoreIsRetriedWhenAStepResultArrivesBeforeItIsDelivered(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes, order := w28aRefuseFirstRestoreRig()
	wflRegisterAll(env, fakes, dfkCallWithFork())
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("p2", "P2", 1))
	var beforeResult wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 4*time.Second, &beforeResult)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("after", "A1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusRunning, beforeResult.Status, "the instance counts as RUNNING in memory although the write was refused")
	firstRestore, retry, delivered := order.indexOf("status:RUNNING", 1), order.indexOf("status:RUNNING", 2), order.indexOf("complete:esc/E1", 1)
	require.NotEqual(t, -1, firstRestore, "events: %v", order.snapshot())
	require.NotEqual(t, -1, retry, "the refused restore was tried again; events: %v", order.snapshot())
	require.NotEqual(t, -1, delivered, "events: %v", order.snapshot())
	assert.Less(t, retry, delivered, "the retry ran before the step result was delivered; events: %v", order.snapshot())
	for _, in := range wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus) {
		assert.Equal(t, wflTenant, in.TenantID)
		assert.Equal(t, wflInstance, in.InstanceID)
	}
}

// An admin pause is accepted while the restore is still pending, because the
// instance counts as RUNNING. The pause and its resume write the stored status
// themselves, so the later step result has nothing left to retry.
func TestExecute_AnAdminPauseAcceptedWhileTheRestoreIsPending_SettlesTheStoredStatusItself(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes, order := w28aRefuseFirstRestoreRig()
	wflRegisterAll(env, fakes, dfkCallWithFork())
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("p2", "P2", 1))
	wflSignalAt(env, 4*time.Second, "instance-pause", wflAdminPause(domain.InitiatorAdmin))
	var whilePaused wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 5*time.Second, &whilePaused)
	wflSignalAt(env, 6*time.Second, "instance-resume", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 3})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("after", "A1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusPaused, whilePaused.Status, "the pause was accepted")
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityPauseInstance))
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityResumeInstance))
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusRunning, domain.InstanceStatusCompleted},
		wflStatuses(fakes), "the refused restore was not repeated: the pause and resume had settled the stored status")
	var admin []string
	for _, e := range order.snapshot() {
		if e == "pause" || e == "resume" || e == "status:RUNNING" || e == "status:DEGRADED" {
			admin = append(admin, e)
		}
	}
	assert.Equal(t, []string{"status:DEGRADED", "status:RUNNING", "pause", "resume"}, admin)
}
