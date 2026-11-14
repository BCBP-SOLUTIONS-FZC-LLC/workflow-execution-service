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

// wflGatewayAfterA is a plan whose department A runs one task, then an
// exclusive gateway over branches; B is the department the gateway may lead to.
func wflGatewayAfterA(branches ...dsl.ExclusiveBranch) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "A", Stages: []dsl.StageDef{{Type: "prep", NodeID: "StepA"}}},
				{ID: "B", Stages: []dsl.StageDef{{Type: "prep", NodeID: "StepB"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"A"}},
				{Exclusive: branches},
			}},
		}},
	}
}

func wflDoneAWith(result string) stageTransitionWire {
	done := wflDone("A", "StepA", 1)
	done.ResultJSON = result
	return done
}

func wflNodesCreated(f *wflFakes) []domain.NodeKey {
	var out []domain.NodeKey
	for _, in := range wflInputs[port.CreateTaskInput](f, port.ActivityCreateTask) {
		out = append(out, in.NodeKey)
	}
	return out
}

// On the first pass an exclusive gateway whose result matches no branch, with
// no else branch, fails the run: the error says so, the instance is written
// FAILED and the department the branch would have led to is never started.
func TestExecute_ExclusiveGatewayMatchingNoBranchAndHavingNoElseFailsTheInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflGatewayAfterA(dsl.ExclusiveBranch{ConditionExpression: `decision == "approve"`, Target: "B"})
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDoneAWith(`{"decision":"reject"}`))

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no exclusive branch matched and no implicit else exists")
	assert.Equal(t, []domain.NodeKey{"A/StepA"}, wflNodesCreated(fakes))
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
	failed := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	assert.Equal(t, wflTenant, failed[0].TenantID)
	assert.Equal(t, wflInstance, failed[0].InstanceID)
}

// A gateway branch that leads, forward or back, to a node or stage that is not
// among the target department's stages fails the run and names the node and
// the department.
func TestExecute_ExclusiveGatewayTargetMissingFromItsDepartmentFailsTheInstance(t *testing.T) {
	cases := []struct {
		name    string
		branch  dsl.ExclusiveBranch
		wantErr string
	}{
		{
			name:    "forward to an unknown node id",
			branch:  dsl.ExclusiveBranch{ConditionExpression: `decision == "go"`, Target: "B", TargetNodeID: "StepNope"},
			wantErr: `node "B/StepNope" not found in department "B" stages`,
		},
		{
			name:    "forward to an unknown stage",
			branch:  dsl.ExclusiveBranch{ConditionExpression: `decision == "go"`, Target: "B", TargetStage: "nope"},
			wantErr: `node "B/nope" not found in department "B" stages`,
		},
		{
			name:    "revert to an unknown node id",
			branch:  dsl.ExclusiveBranch{ConditionExpression: `decision == "go"`, RevertToDept: "A", RevertToNodeID: "StepGone"},
			wantErr: `node "A/StepGone" not found in department "A" stages`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			collab := wflGatewayAfterA(tc.branch)
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, collab)
			wflSignalAt(env, 5*time.Second, "stage-transition", wflDoneAWith(`{"decision":"go"}`))

			_, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Equal(t, []domain.NodeKey{"A/StepA"}, wflNodesCreated(fakes), "no task is created at the missing target")
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
		})
	}
}
