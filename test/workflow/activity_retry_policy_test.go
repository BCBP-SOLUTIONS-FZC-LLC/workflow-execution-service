package workflow_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflOneTaskCollab() *dsl.CompiledCollaboration {
	return wflOneDept("ops", dsl.StageDef{Type: "prep", NodeID: "Step"})
}

// A database-write activity that fails with an ordinary error is retried
// under the backoff policy, and the workflow keeps waiting for it.
func TestExecute_DBWriteActivityPlainErrorIsRetriedUntilItSucceeds(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneTaskCollab()
	fakes := wflNewFakes()
	fakes.failWith = func(name string, attempt int) error {
		if name == port.ActivityCreateTask && attempt <= 2 {
			return errors.New("connection reset")
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 30*time.Second, "stage-transition", wflDone("ops", "Step", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, 3, fakes.attemptsOf(port.ActivityCreateTask), "two failures and the attempt that succeeded")
	creates := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	for _, in := range creates {
		assert.Equal(t, wflTenant, in.TenantID)
		assert.Equal(t, wflInstance, in.InstanceID)
		assert.Equal(t, domain.NodeKey("ops/Step"), in.NodeKey)
		assert.EqualValues(t, 1, in.VisitCount, "a retry carries the same visit count, it is not a revisit")
	}
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusCompleted}, wflStatuses(fakes))
}

// A database-write activity that fails with an application error of type
// ValidationError, NotFoundError or VersionConflict is not retried: the error
// reaches the workflow after one attempt and the instance fails.
func TestExecute_DBWriteActivityErrorOfAMappedTypeIsNotRetried(t *testing.T) {
	for _, errType := range []string{"ValidationError", "NotFoundError", "VersionConflict"} {
		t.Run(errType, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			collab := wflOneTaskCollab()
			fakes := wflNewFakes()
			fakes.failWith = func(name string, _ int) error {
				if name == port.ActivityCreateTask {
					return temporal.NewApplicationError("rejected by the database layer", errType)
				}
				return nil
			}
			wflRegisterAll(env, fakes, collab)

			_, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "workflow: CreateTaskActivity: ")
			assert.Contains(t, err.Error(), "rejected by the database layer")
			assert.Equal(t, 1, fakes.attemptsOf(port.ActivityCreateTask))
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
			failed := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
			assert.Equal(t, wflTenant, failed[0].TenantID)
			assert.Equal(t, wflInstance, failed[0].InstanceID)
		})
	}
}

// A completion write that gives up ends the run: the error comes back from the
// stage once, the task is not marked completed, the next step is never
// reached and the instance is written FAILED.
func TestExecute_CompleteAssignmentFailureFailsTheRunWithoutCompletingTheTask(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops",
		dsl.StageDef{Type: "prep", NodeID: "First"},
		dsl.StageDef{Type: "prep", NodeID: "Second"},
	)
	fakes := wflNewFakes()
	fakes.failWith = func(name string, _ int) error {
		if name == port.ActivityCompleteAssignment {
			return temporal.NewApplicationError("task row not found", "NotFoundError")
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	done := wflDone("ops", "First", 1)
	done.UserID = "user-7"
	done.RecordVersion = 4
	done.ResultJSON = `{"approved":true}`
	wflSignalAt(env, 30*time.Second, "stage-transition", done)

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "workflow: CompleteAssignmentActivity: ")
	assert.Contains(t, err.Error(), "task row not found")
	completions := wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment)
	require.Len(t, completions, 1, "a not-found answer is not retried")
	assert.Equal(t, port.CompleteAssignmentInput{
		TaskID: "ops/First", UserID: "user-7", TenantID: wflTenant, ResultJSON: `{"approved":true}`, RecordVersion: 4,
	}, completions[0])
	for _, in := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		assert.NotEqual(t, domain.TaskStatusCompleted, in.Status, "task %s was marked completed", in.TaskID)
	}
	creates := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	require.Len(t, creates, 1, "the second step must not be reached")
	assert.Equal(t, domain.NodeKey("ops/First"), creates[0].NodeKey)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
}

// wflUpdateAnswer records how a workflow update was answered.
type wflUpdateAnswer struct {
	mu        sync.Mutex
	completed bool
	err       error
}

func (a *wflUpdateAnswer) Accept()      {}
func (a *wflUpdateAnswer) Reject(error) {}
func (a *wflUpdateAnswer) Complete(_ any, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.completed, a.err = true, err
}

func (a *wflUpdateAnswer) state() (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.completed, a.err
}

// A defer whose database write keeps failing with an ordinary error gets no
// answer while the activity is retried; it is answered only once the write
// succeeds, and the same input is sent on every attempt.
func TestExecute_DeferUpdateStaysUnansweredWhileTheDeferWriteFailsRetryably(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops",
		dsl.StageDef{Type: "prep", NodeID: "First"},
		dsl.StageDef{Type: "prep", NodeID: "Second"},
	)
	fakes := wflNewFakes()
	fakes.failWith = func(name string, attempt int) error {
		if name == port.ActivityDeferTask && attempt <= 4 {
			return errors.New("database unavailable")
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("ops", "First", 1))

	answer := &wflUpdateAnswer{}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("stage-defer:"+wflInstance, "defer-1", answer, deferWire{
			DeptID: "ops", FromStage: "Second", Reason: "needs rework", UserID: "user-3", RecordVersion: 5,
			TaskID: "ops/Second", AssignmentID: "assign-9",
		})
	}, 5*time.Second)

	var midRetryAttempts int
	var midRetryAnswered bool
	env.RegisterDelayedCallback(func() {
		midRetryAttempts = fakes.attemptsOf(port.ActivityDeferTask)
		midRetryAnswered, _ = answer.state()
	}, 10*time.Second)

	wflSignalAt(env, 60*time.Second, "stage-transition", wflDone("ops", "First", 2))
	wflSignalAt(env, 90*time.Second, "stage-transition", wflDone("ops", "Second", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.GreaterOrEqual(t, midRetryAttempts, 2, "the write was being retried")
	assert.False(t, midRetryAnswered, "no answer while the write is still failing")
	answered, answerErr := answer.state()
	assert.True(t, answered)
	require.NoError(t, answerErr)
	defers := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, defers, 5, "four failures and the attempt that succeeded")
	for _, in := range defers {
		assert.Equal(t, port.DeferTaskInput{
			TaskID: "ops/Second", TenantID: wflTenant, UserID: "user-3", AssignmentID: "assign-9",
			Reason: "needs rework", DeferredToNodeKey: "ops/First", RecordVersion: 5,
		}, in)
	}
}
