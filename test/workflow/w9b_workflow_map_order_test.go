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

// w9bFiveBranchFork is an intake step followed by a fork of five departments
// whose names are not in alphabetical order, so that the order in which their
// tasks open differs from the sorted order.
func w9bFiveBranchFork(stage func(dept string) dsl.StageDef) *dsl.CompiledCollaboration {
	names := []string{"e", "b", "d", "a", "c"}
	departments := []dsl.DepartmentDef{{ID: "intake", Stages: wflStages("Intake")}}
	var branches []dsl.ParallelBranch
	for _, n := range names {
		departments = append(departments, dsl.DepartmentDef{ID: n, Stages: []dsl.StageDef{stage(n)}})
		branches = append(branches, dsl.ParallelBranch{DeptID: n, Steps: []dsl.ExecutionStep{{Sequential: []string{n}}}})
	}
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: departments,
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"intake"}},
				{Parallel: branches},
			}},
		}},
	}
}

// The tasks a force-back abandons are reported in sorted order whatever order
// the branches opened them in, so the same history always records the same
// route.
func TestExecute_ForceBackOverManyOpenBranchTasksNamesThemInSortedOrder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w9bFiveBranchFork(func(string) dsl.StageDef { return wflStages("T")[0] }))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-7", RecordVersion: 9})
	wflCancelAt(env, 8*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, wflInstance, audits[0].InstanceID)
	assert.Equal(t, []domain.NodeKey{"a/T", "b/T", "c/T", "d/T", "e/T"}, audits[0].OldNodeKeys)
}

// The keys of the outside waits an instance is in are answered in sorted
// order, whatever order the branches began waiting in.
func TestExecute_WaitingExternalQueryOverManyBranchesAnswersInSortedOrder(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := w9bFiveBranchFork(func(string) dsl.StageDef {
		return dsl.StageDef{Type: "receive_task", NodeID: "Wait", Extras: map[string]string{"message": "answer-received"}}
	})
	wflRegisterAll(env, fakes, collab)
	var waiting []string
	taken := false
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
	wflQueryWaitingAt(t, env, 5*time.Second, &waiting, &taken)
	wflCancelAt(env, 8*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	require.True(t, taken, "the query was answered")
	assert.Equal(t, []string{"a/Wait", "b/Wait", "c/Wait", "d/Wait", "e/Wait"}, waiting)
}
