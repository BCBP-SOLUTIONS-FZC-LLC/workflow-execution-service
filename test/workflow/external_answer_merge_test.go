package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// An outside answer whose variables cannot be merged, because the instance's
// stored context is not a JSON object, is dropped with a warning and the step
// keeps waiting: the waiting-external query still lists the receive task and
// the step after it is not started.
func TestExecute_OutsideAnswerIsDroppedAndTheStepKeepsWaitingWhenTheStoredContextIsNotAJSONObject(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("Ops",
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)

	var waitingBefore, waitingAfter []string
	var tookBefore, tookAfter bool
	wflQueryWaitingAt(t, env, 1*time.Second, &waitingBefore, &tookBefore)
	wflSignalAt(env, 2*time.Second, port.SignalInstanceExternalAnswer, wflAnswerWire{
		Key: "Ops/Receive_Bid", Variables: map[string]any{"bid": "42"},
	})
	wflQueryWaitingAt(t, env, 4*time.Second, &waitingAfter, &tookAfter)
	wflSignalAt(env, 8*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 2})

	status, err := wflStart(t, env, fakes, `[1,2,3]`)

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status, "only the cancel ended the run: the answer did not complete the receive")
	require.True(t, tookBefore && tookAfter)
	assert.Equal(t, []string{"Ops/Receive_Bid"}, waitingBefore)
	assert.Equal(t, []string{"Ops/Receive_Bid"}, waitingAfter, "the wait stays open after the dropped answer")
	assert.Empty(t, wflNodesCreated(fakes), "Award must not start on a dropped answer")
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
}
