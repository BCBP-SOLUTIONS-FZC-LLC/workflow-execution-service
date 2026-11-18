package workflow_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// A cancel that lands while the task of a stage is being created ends the
// instance at once: the cancel is written once, the abandoned stage never
// records itself as the instance's current node, and nothing runs after it.
func TestExecute_CancelWhileATaskIsBeingCreatedEndsTheInstanceWithoutRecordingTheStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflOneDept("ops", wflStages("Review", "Approve")...))
	var once sync.Once
	fakes.failFor = func(_ string, in any) error {
		if _, ok := in.(port.CreateTaskInput); ok {
			once.Do(func() {
				env.SignalWorkflow("instance-cancel:"+wflInstance, adminSignalWire{AdminUserID: "admin-9", RecordVersion: 4})
			})
		}
		return nil
	}

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	require.Equal(t, []domain.NodeKey{"ops/Review"}, wflNodesCreated(fakes), "the second stage never started")
	assert.Zero(t, fakes.attemptsOf(port.ActivityUpdateInstanceNodes), "the abandoned stage was never recorded as current")
}
