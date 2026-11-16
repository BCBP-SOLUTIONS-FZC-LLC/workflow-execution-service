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

// A revert that names only a department sends the run back to that department
// and keeps the history of the departments before it: a later force-back past
// that department still finds the earlier department to return to.
func TestExecute_DepartmentOnlyRevertKeepsTheHistoryBeforeThatDepartment(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "X", Stages: wflStages("X1")},
				{ID: "A", Stages: wflStages("A1")},
				{ID: "B", Stages: wflStages("B1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"X", "A"}},
				{Exclusive: []dsl.ExclusiveBranch{{ConditionExpression: `decision == "redo"`, RevertToDept: "A"}}},
				{Sequential: []string{"B"}},
			}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	redo := wflDone("A", "A1", 1)
	redo.ResultJSON = `{"decision":"redo"}`
	ok := wflDone("A", "A1", 2)
	ok.ResultJSON = `{"decision":"ok"}`
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("X", "X1", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", redo)
	wflSignalAt(env, 4*time.Second, "stage-transition", ok)
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 3})
	wflSignalAt(env, 8*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 4})
	wflCancelAt(env, 12*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	created := wflNodesCreated(fakes)
	assert.Equal(t, 2, countNode(created, "X/X1"), "the second force-back found the first department in the history")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 2)
	assert.Equal(t, "A/A1", audits[0].TargetNodeID)
	assert.Equal(t, "X/X1", audits[1].TargetNodeID)
}

func countNode(nodes []domain.NodeKey, want domain.NodeKey) int {
	n := 0
	for _, k := range nodes {
		if k == want {
			n++
		}
	}
	return n
}
