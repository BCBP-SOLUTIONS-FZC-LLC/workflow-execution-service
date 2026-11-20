package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflGatewayFirst(branches ...dsl.ExclusiveBranch) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "B", Stages: []dsl.StageDef{{Type: "prep", NodeID: "StepB"}}},
				{ID: "C", Stages: []dsl.StageDef{{Type: "prep", NodeID: "StepC"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Exclusive: branches}}},
		}},
	}
}

func TestExecute_ExclusiveGatewayFirstInListTakesTheElseBranch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflGatewayFirst(
		dsl.ExclusiveBranch{ConditionExpression: `status != "x"`, Target: "B"},
		dsl.ExclusiveBranch{Target: "C"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("C", "StepC", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"C/StepC"}, wflNodesCreated(fakes))
}
