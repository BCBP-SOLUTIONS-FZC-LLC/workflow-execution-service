package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestUpdateInstanceNodes_RetryWithSameKeys_SucceedsAndLeavesTheSameKeys(t *testing.T) {
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	in := port.UpdateInstanceNodesInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(),
		NodeKeys: []domain.NodeKey{"sales/review", "sales/approve"},
	}

	require.NoError(t, deps.UpdateInstanceNodes(context.Background(), in))
	require.NoError(t, deps.UpdateInstanceNodes(context.Background(), in), "a retry after a committed write is not a version conflict")

	want := []string{"sales/review", "sales/approve"}
	require.Len(t, world.nodeWrites, 2)
	for _, w := range world.nodeWrites {
		assert.Equal(t, tenant, w.Tenant)
		assert.Equal(t, inst.ID, w.ID)
		assert.Equal(t, want, w.Keys)
	}
	assert.Equal(t, want, world.instances[inst.ID].CurrentNodeKeys)
	world.requireAllCallsForTenant(t, tenant)
}

func TestUpdateInstanceNodes_NoNodeKeys_WritesAnEmptyNonNilListAtTheVersionJustRead(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		keys []domain.NodeKey
	}{
		{name: "nil", keys: nil},
		{name: "empty", keys: []domain.NodeKey{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, _ := wkactNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, domain.InstanceStatusRunning)
			inst.CurrentNodeKeys = []string{"sales/review"}
			inst.RecordVersion = 7

			err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
				InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKeys: tc.keys,
			})

			require.NoError(t, err)
			require.Len(t, world.nodeWrites, 1)
			w := world.nodeWrites[0]
			assert.NotNil(t, w.Keys, "an empty list is written as an empty array, never as null")
			assert.Empty(t, w.Keys)
			assert.Equal(t, int64(7), w.Version)
			assert.Equal(t, tenant, w.Tenant)
			assert.Equal(t, inst.ID, w.ID)
			assert.Empty(t, world.instances[inst.ID].CurrentNodeKeys, "the previous current nodes are cleared")
		})
	}
}

func TestRecordForceRoute_RetryAfterCommit_DoesNotSupersedeTheBypassedTaskAgain(t *testing.T) {
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	bypassed := world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	assignment := world.addAssignment(tenant, bypassed, true)
	in := port.RecordForceRouteInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), OldNodeKeys: []domain.NodeKey{"sales/review"},
		TargetNodeID: "sales/approve", AdminUserID: uuid.NewString(),
	}

	require.NoError(t, deps.RecordForceRoute(context.Background(), in))
	versionAfterFirst := world.tasks[bypassed.ID].RecordVersion
	updatesAfterFirst := len(world.callsTo("Tasks.UpdateStatus"))
	vacatesAfterFirst := len(world.callsTo("Assignments.Vacate"))
	require.NoError(t, deps.RecordForceRoute(context.Background(), in))

	assert.Equal(t, 2, tx.committed)
	assert.Equal(t, domain.TaskStatusSuperseded, world.tasks[bypassed.ID].Status)
	assert.Equal(t, versionAfterFirst, world.tasks[bypassed.ID].RecordVersion, "the retry does not touch the already superseded task")
	assert.Len(t, world.callsTo("Tasks.UpdateStatus"), updatesAfterFirst)
	assert.Len(t, world.callsTo("Assignments.Vacate"), vacatesAfterFirst)
	assert.False(t, world.assignments[assignment.ID].IsActive)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskSuperseded), 1, "one superseded event for the one task")
	world.requireAllCallsForTenant(t, tenant)
}
