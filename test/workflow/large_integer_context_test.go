package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestExecute_OutsideAnswerWritesIntegersAbove2Pow53Verbatim(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops",
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, port.SignalInstanceExternalAnswer, wflAnswerWire{
		Key: "ops/Receive_Bid", Variables: map[string]any{"bid": json.Number("9007199254740995")},
	})
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("ops", "Award", 1))

	status, err := wflStart(t, env, fakes, `{"account":9007199254740993}`)

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	tasks := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	require.Len(t, tasks, 1)
	assert.Equal(t, `{"account":9007199254740993,"bid":9007199254740995}`, tasks[0].ContextJSON)
}
