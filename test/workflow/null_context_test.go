package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"go.temporal.io/sdk/testsuite"
)

// An instance started with a context of JSON null runs like one started with
// an empty context: an input mapping writes into an empty set of variables
// instead of failing the workflow task, and the task is created with the
// mapped variable.
func TestExecute_NullContextRunsLikeAnEmptyContext(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops", wflStages("Prepare")...)
	collab.Plans[0].Execution.Steps[0].IOMapping = &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=missing", Target: "copy"}}}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("ops", "Prepare", 1))

	status, err := wflStart(t, env, fakes, "null")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	tasks := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	require.Len(t, tasks, 1)
	assert.JSONEq(t, `{"copy":null}`, tasks[0].ContextJSON)
	assert.Equal(t, wflTenant, tasks[0].TenantID)
	assert.Equal(t, wflInstance, tasks[0].InstanceID)
}

// An outside answer merged into an instance whose context is JSON null adds
// its variables to an empty set instead of failing the workflow task.
func TestExecute_OutsideAnswerMergesIntoANullContext(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops",
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, port.SignalInstanceExternalAnswer, wflAnswerWire{Key: "ops/Receive_Bid", Variables: map[string]any{"bid": 7}})
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("ops", "Award", 1))

	status, err := wflStart(t, env, fakes, "null")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	tasks := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	require.Len(t, tasks, 1)
	assert.JSONEq(t, `{"bid":7}`, tasks[0].ContextJSON)
}
