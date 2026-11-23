package workflow_test

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	w19bBillingIAMID  = "018e1f2a-0000-7000-8000-0000000000b1"
	w19bShippingIAMID = "018e1f2a-0000-7000-8000-0000000000c2"
)

// w19bThreeBranchFork is a fork of warehouse, billing and shipping, one task each.
func w19bThreeBranchFork() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "warehouse", Stages: []dsl.StageDef{{Type: "prep"}}},
				{ID: "billing", IAMDepartmentID: w19bBillingIAMID, Stages: []dsl.StageDef{{Type: "prep"}}},
				{ID: "shipping", IAMDepartmentID: w19bShippingIAMID, Stages: []dsl.StageDef{{Type: "prep"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
				{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
				{DeptID: "shipping", Steps: []dsl.ExecutionStep{{Sequential: []string{"shipping"}}}},
			}}}},
		}},
	}
}

// A force-forward naming the node where a branch of a DEGRADED instance failed
// takes that branch out of the failed set and counts it as completed: the audit
// record names the last node the branch completed and the node it was sent to,
// and the instance leaves DEGRADED and finishes.
func TestExecute_ForceForwardOfAFailedBranchOfADegradedInstance_IsAuditedWithItsLastCompletedNodeAndTheInstanceFinishes(t *testing.T) {
	env := w19bNewEnv(&w19bWarnings{})
	fakes := wflNewFakes()
	wflFailTaskOnce(fakes, "billing/X2")
	wflRegisterAll(env, fakes, wflTwoDeptBranch())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 3*time.Second, "stage-transition", wflDone("prev", "P1", 1))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("billing", "X1", 1))
	wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/X2", RecordVersion: 7})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1, "one audit record")
	assert.Equal(t, port.RecordForceRouteInput{
		InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"billing/X1"},
		TargetNodeID: "billing/X2", Direction: domain.ForceRouteDirectionForward, AdminUserID: "admin-1", RecordVersion: 7,
	}, routes[0])
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusRunning, domain.InstanceStatusCompleted}, wflStatuses(fakes))
	assert.Equal(t, 1, w19bCountNode(wflNodesCreated(fakes), "billing/X2"), "the failed stage is not created again: the branch was forwarded, not retried")
}

func w19bCountNode(nodes []domain.NodeKey, node domain.NodeKey) int {
	n := 0
	for _, got := range nodes {
		if got == node {
			n++
		}
	}
	return n
}

// An instance with several failed branches stays DEGRADED when one of them is
// force-forwarded and goes to RUNNING when the last one is. What the stored
// failed-branch list holds between the two forwards is not asserted.
func TestExecute_ADegradedInstanceReturnsToRunningOnlyWhenItsLastFailedBranchIsForwarded(t *testing.T) {
	env := w19bNewEnv(&w19bWarnings{})
	fakes := wflNewFakes()
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && (v.NodeKey == "billing/prep" || v.NodeKey == "shipping/prep") {
			return wflRefuse("create " + string(v.NodeKey))
		}
		return nil
	}
	wflRegisterAll(env, fakes, w19bThreeBranchFork())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7})
	var afterFirst []domain.InstanceStatus
	wflclock.After(env, func() { afterFirst = wflStatuses(fakes) }, 6500*time.Millisecond)
	wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-2", TargetNodeKey: "shipping/prep", RecordVersion: 8})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	require.NotEmpty(t, afterFirst)
	assert.NotContains(t, afterFirst, domain.InstanceStatusRunning, "one failed branch left: no RUNNING write yet")
	assert.Equal(t, domain.InstanceStatusDegraded, afterFirst[len(afterFirst)-1], "one failed branch left: still DEGRADED")
	statuses := wflStatuses(fakes)
	require.GreaterOrEqual(t, len(statuses), 2)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusCompleted}, statuses[len(statuses)-2:], "RUNNING after the last forward, then COMPLETED")
	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	require.NotEmpty(t, writes)
	var failedDepts []uuid.UUID
	for _, b := range writes[0].FailedBranches {
		failedDepts = append(failedDepts, b.DepartmentID)
	}
	assert.Equal(t, domain.InstanceStatusDegraded, writes[0].Status)
	assert.ElementsMatch(t, []uuid.UUID{uuid.MustParse(w19bBillingIAMID), uuid.MustParse(w19bShippingIAMID)}, failedDepts)
	runningAt := slices.IndexFunc(writes, func(in port.UpdateInstanceStatusInput) bool { return in.Status == domain.InstanceStatusRunning })
	require.NotEqual(t, -1, runningAt, "a RUNNING write is made")
	assert.Empty(t, writes[runningAt].FailedBranches, "the RUNNING write carries no failed branch")
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 2)
	assert.Equal(t, []string{"billing/prep", "shipping/prep"}, []string{routes[0].TargetNodeID, routes[1].TargetNodeID})
}
