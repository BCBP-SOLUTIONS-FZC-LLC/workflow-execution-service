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

func TestExecute_IOMappingKeepsIntegersAbove2Pow53Verbatim(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops", wflStages("Prepare")...)
	collab.Plans[0].Execution.Steps[0].IOMapping = &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=amount", Target: "copy"}}}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("ops", "Prepare", 1))

	status, err := wflStart(t, env, fakes, `{"amount":9007199254740993}`)

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	tasks := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	require.Len(t, tasks, 1)
	assert.Equal(t, `{"amount":9007199254740993,"copy":9007199254740993}`, tasks[0].ContextJSON)
}
