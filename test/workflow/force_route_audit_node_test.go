package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflStages(names ...string) []dsl.StageDef {
	stages := make([]dsl.StageDef, len(names))
	for i, n := range names {
		stages[i] = dsl.StageDef{Type: "prep", NodeID: n}
	}
	return stages
}

func wflNodeWrites(f *wflFakes) [][]domain.NodeKey {
	var out [][]domain.NodeKey
	for _, in := range wflInputs[port.UpdateInstanceNodesInput](f, port.ActivityUpdateInstanceNodes) {
		out = append(out, in.NodeKeys)
	}
	return out
}

// A force-forward at the top level re-enters the target's department at its
// first stage, so the route record and the instance's current node name that
// first stage, not the later stage the admin pointed at.
func TestExecute_TopLevelForceForwardRecordsTheDepartmentsFirstStageThatRuns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "deptA", Stages: wflStages("A1")},
				{ID: "deptB", Stages: wflStages("B1", "B2")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"deptA", "deptB"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "deptB/B2", RecordVersion: 3})
	var writesAtJump [][]domain.NodeKey
	wflclock.After(env, func() { writesAtJump = wflNodeWrites(fakes) }, 8*time.Second)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("deptB", "B1", 1))
	wflSignalAt(env, 15*time.Second, "stage-transition", wflDone("deptB", "B2", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, port.RecordForceRouteInput{
		InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"deptA/A1"},
		TargetNodeID: "deptB/B1", Direction: domain.ForceRouteDirectionForward, AdminUserID: "admin-1", RecordVersion: 3,
	}, routes[0])
	require.NotEmpty(t, writesAtJump)
	for _, w := range writesAtJump {
		assert.NotEqual(t, []domain.NodeKey{"deptB/B2"}, w, "B2 never ran when the department re-entered")
	}
	assert.Equal(t, []domain.NodeKey{"deptB/B1"}, writesAtJump[len(writesAtJump)-1])
}

// A force-back at the top level runs the popped node's department again from
// its first stage, so the route record names that first stage, not the later
// popped node.
func TestExecute_TopLevelForceBackRecordsTheDepartmentsFirstStageThatRuns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops", wflStages("First", "Second", "Third")...)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("ops", "First", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("ops", "Second", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-6", RecordVersion: 11})
	wflSignalAt(env, 20*time.Second, "stage-transition", wflDone("ops", "First", 2))
	wflSignalAt(env, 30*time.Second, "stage-transition", wflDone("ops", "Second", 2))
	wflSignalAt(env, 40*time.Second, "stage-transition", wflDone("ops", "Third", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, port.RecordForceRouteInput{
		InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"ops/Third"},
		TargetNodeID: "ops/First", Direction: domain.ForceRouteDirectionBack, AdminUserID: "admin-6", RecordVersion: 11,
	}, routes[0])
}

func wflBillingBranchCollab(billingSteps ...string) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "warehouse", Stages: wflStages("W1")},
				{ID: "billing", Stages: wflStages("B1", "B2")},
				{ID: "shipping", Stages: wflStages("S1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
				{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: billingSteps}}},
			}}}},
		}},
	}
}

// Inside a parallel step a force-forward resolves the branch without running
// the target stage, so the route record names the node the branch had open (as
// the node left and the node reached), never a stage that did not run.
func TestExecute_ParallelForceForwardToALaterStageRecordsTheNodeTheBranchHadOpen(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflBillingBranchCollab("billing"))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-2", TargetNodeKey: "billing/B2", RecordVersion: 4})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "W1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, port.RecordForceRouteInput{
		InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"billing/B1"},
		TargetNodeID: "billing/B1", Direction: domain.ForceRouteDirectionForward, AdminUserID: "admin-2", RecordVersion: 4,
	}, routes[0])
}

// A target that lies in a later department of the same branch leaves the
// record with the branch's open node as the node left, not an empty list.
func TestExecute_ParallelForceForwardToALaterDepartmentOfTheBranchRecordsTheOpenNode(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflBillingBranchCollab("billing", "shipping"))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-2", TargetNodeKey: "shipping/S1", RecordVersion: 4})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("warehouse", "W1", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, []domain.NodeKey{"billing/B1"}, routes[0].OldNodeKeys)
	assert.Equal(t, "billing/B1", routes[0].TargetNodeID)
	assert.Equal(t, wflTenant, routes[0].TenantID)
	assert.Equal(t, wflInstance, routes[0].InstanceID)
}
