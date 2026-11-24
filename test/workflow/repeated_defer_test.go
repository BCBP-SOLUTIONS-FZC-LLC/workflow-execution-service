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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
)

func wflDeferBackSend(update string, env *testsuite.TestWorkflowEnvironment, answer *updateAnswer) func() {
	return func() {
		env.UpdateWorkflow("stage-defer:"+wflInstance, update, answer, deferWire{
			DeptID: "deptB", FromStage: "prep", Reason: "need more info", UserID: "user-1", RecordVersion: 1,
			TaskID: "deptB/B1", AssignmentID: "assign-1",
		})
	}
}

func wflDeferBack(update string, env *testsuite.TestWorkflowEnvironment, delay time.Duration, answer *updateAnswer) {
	wflclock.After(env, wflDeferBackSend(update, env, answer), delay)
}

// A second defer of a task whose first defer is still being applied changes
// nothing: the caller hears a refusal (so the service answers 409), the task is
// deferred once and the instance goes back one step only.
func TestExecute_SecondDeferOfTheSameTaskIsRefusedAndRoutesNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflTwoDeptsCollab())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("deptA", "A1", 1))
	first, second := &updateAnswer{}, &updateAnswer{}
	wflclock.Together(env, 5*time.Second,
		wflDeferBackSend("defer-1", env, first),
		wflDeferBackSend("defer-2", env, second),
	)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("deptA", "A1", 2))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDone("deptB", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.True(t, first.completed && first.err == nil && first.rejected == nil, "the first defer was applied: %+v", first)
	refusal := second.rejected
	if refusal == nil {
		refusal = second.err
	}
	require.Error(t, refusal, "the second defer was refused: %+v", second)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, refusal, &appErr)
	assert.Equal(t, port.UpdateRefusedErrorType, appErr.Type())
	deferred := wflInputs[port.DeferTaskInput](fakes, port.ActivityDeferTask)
	require.Len(t, deferred, 1)
	assert.Equal(t, wflTenant, deferred[0].TenantID)
	assert.Equal(t, "deptB/B1", deferred[0].TaskID)
	assert.Equal(t, []domain.NodeKey{"deptA/A1", "deptB/B1", "deptA/A1", "deptB/B1"}, wflNodesCreated(fakes), "the instance went back one step, once")
}
