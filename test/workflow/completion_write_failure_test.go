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
)

func TestExecute_TheFinalCompletedWriteThatGivesUpEndsTheRunWithTheError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneTaskCollab()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		if update, ok := in.(port.UpdateInstanceStatusInput); ok && name == port.ActivityUpdateInstanceStatus &&
			update.Status == domain.InstanceStatusCompleted {
			return temporal.NewApplicationError("rejected by the database layer", "NotFoundError")
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 30*time.Second, "stage-transition", wflDone("ops", "Step", 1))

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err, "the instance is not recorded as completed, so the run does not report success")
	assert.Contains(t, err.Error(), "rejected by the database layer")
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityUpdateInstanceStatus), "a mapped error is not retried")
}
