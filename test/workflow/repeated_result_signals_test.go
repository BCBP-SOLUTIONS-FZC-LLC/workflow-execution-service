package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// The same step result sent three times in a row is completed once and never
// stops the signal router: a cancel that arrives later is still handled, and
// the later stage is not completed by the repeats.
func TestExecute_RepeatedStepResultsNeverBlockTheSignalRouter(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflOneDept("ops", wflStages("First", "Second")...))
	first := wflSignal{"stage-transition", wflDone("ops", "First", 1)}
	wflSignalsAt(env, 2*time.Second, first, first, first)
	wflCancelAt(env, 10*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityCompleteAssignment), "the first stage was completed once")
	assert.Equal(t, []domain.NodeKey{"ops/First", "ops/Second"}, wflNodesCreated(fakes))
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
}
