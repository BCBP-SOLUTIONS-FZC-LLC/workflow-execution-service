package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestExecute_DeferWithAStaleVersionReachesTheCallerAsAVersionConflict(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failWith = func(name string, _ int) error {
		if name == port.ActivityDeferTask {
			return temporal.NewNonRetryableApplicationError(
				"defer: lost the record_version race", port.VersionConflictErrorType, nil)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflTwoDeptsCollab())
	answer := &updateAnswer{}
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("deptA", "A1", 1))
	wflDeferBack("defer-1", env, 5*time.Second, answer)
	wflCancelAt(env, 8*time.Second)

	_, _ = wflStart(t, env, fakes, "")

	failure := answer.rejected
	if failure == nil {
		failure = answer.err
	}
	require.Error(t, failure, "the defer failed: %+v", answer)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, failure, &appErr)
	assert.Equal(t, port.VersionConflictErrorType, appErr.Type())
}
