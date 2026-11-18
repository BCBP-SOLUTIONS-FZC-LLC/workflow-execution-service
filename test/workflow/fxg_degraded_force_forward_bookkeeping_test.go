package workflow_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fxgFailBillingAndShippingFirstTasks fails the first task of the billing and shipping branches, so neither
// branch ever completes a node.
func fxgFailBillingAndShippingFirstTasks(fakes *wflFakes) {
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && (v.NodeKey == "billing/prep" || v.NodeKey == "shipping/prep") {
			return wflRefuse("create " + string(v.NodeKey))
		}
		return nil
	}
}

func fxgForwardBothFailedBranches(t *testing.T) *wflFakes {
	t.Helper()
	env := w19bNewEnv(&w19bWarnings{})
	fakes := wflNewFakes()
	fxgFailBillingAndShippingFirstTasks(fakes)
	wflRegisterAll(env, fakes, w19bThreeBranchFork())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7})
	wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-2", TargetNodeKey: "shipping/prep", RecordVersion: 8})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	require.Equal(t, domain.InstanceStatusCompleted, status)
	return fakes
}

// A branch that failed on its first task has completed no node, so the audit has no old node key to name.
func TestExecute_ForceForwardOfABranchThatFailedOnItsFirstTask_AuditsNoOldNodeKey(t *testing.T) {
	fakes := fxgForwardBothFailedBranches(t)

	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)

	require.Len(t, routes, 2)
	for _, route := range routes {
		assert.Empty(t, route.OldNodeKeys, "no node was completed, so no old node key is recorded")
		assert.Equal(t, domain.ForceRouteDirectionForward, route.Direction)
	}
	assert.Equal(t, []string{"billing/prep", "shipping/prep"}, []string{routes[0].TargetNodeID, routes[1].TargetNodeID})
}

// The stored failed-branch list follows each forward: after the first forward only the other branch is listed,
// and the write that returns the instance to RUNNING lists none.
func TestExecute_ForwardingOneOfTwoFailedBranches_RewritesTheStoredFailedBranchList(t *testing.T) {
	fakes := fxgForwardBothFailedBranches(t)

	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)

	fxgAssertWritesAreForTheInstance(t, writes)
	require.GreaterOrEqual(t, len(writes), 4, "entry, after the first forward, running, completed")
	assert.Equal(t, domain.InstanceStatusDegraded, writes[0].Status)
	assert.Len(t, writes[0].FailedBranches, 2)
	assert.Equal(t, domain.InstanceStatusDegraded, writes[1].Status, "the instance stays DEGRADED after the first forward")
	require.Len(t, writes[1].FailedBranches, 1, "the forwarded branch is no longer listed")
	assert.Equal(t, uuid.MustParse(w19bShippingIAMID), writes[1].FailedBranches[0].DepartmentID)
	assert.Equal(t, domain.InstanceStatusRunning, writes[2].Status, "the last forward returns the instance to RUNNING")
	assert.Empty(t, writes[2].FailedBranches)
}

func fxgAssertWritesAreForTheInstance(t *testing.T, writes []port.UpdateInstanceStatusInput) {
	t.Helper()
	for i, w := range writes {
		assert.Equal(t, wflInstance, w.InstanceID, "status write %d", i)
		assert.Equal(t, wflTenant, w.TenantID, "status write %d", i)
	}
}

// A branch that recovers after a force-back leaves the stored failed-branch list while the other branch is
// still failed: the instance never lists a branch that has recovered.
func TestExecute_ARespawnedBranchThatSucceeds_IsRemovedFromTheStoredFailedBranchList(t *testing.T) {
	env := w19bNewEnv(&w19bWarnings{})
	fakes := wflNewFakes()
	billingFailed := false
	fakes.failFor = func(_ string, in any) error {
		v, ok := in.(port.CreateTaskInput)
		switch {
		case ok && v.NodeKey == "billing/prep" && !billingFailed:
			billingFailed = true
			return wflRefuse("create billing/prep")
		case ok && v.NodeKey == "shipping/prep":
			return wflRefuse("create shipping/prep")
		}
		return nil
	}
	wflRegisterAll(env, fakes, w19bThreeBranchFork())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 7})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDoneStage("billing", "prep", 2))
	wflSignalAt(env, 12*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-2", TargetNodeKey: "shipping/prep", RecordVersion: 8})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	require.Equal(t, domain.InstanceStatusCompleted, status)
	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	fxgAssertWritesAreForTheInstance(t, writes)
	require.GreaterOrEqual(t, len(writes), 3, "entry, after the recovery, running")
	assert.Len(t, writes[0].FailedBranches, 2)
	assert.Equal(t, domain.InstanceStatusDegraded, writes[1].Status, "the instance stays DEGRADED while shipping is failed")
	require.Len(t, writes[1].FailedBranches, 1, "the recovered branch is no longer listed")
	assert.Equal(t, uuid.MustParse(w19bShippingIAMID), writes[1].FailedBranches[0].DepartmentID)
	assert.Equal(t, domain.InstanceStatusRunning, writes[2].Status)
}
