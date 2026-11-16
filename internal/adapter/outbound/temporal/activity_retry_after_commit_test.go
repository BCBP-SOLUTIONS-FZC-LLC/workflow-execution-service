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

func TestUpdateInstanceStatus_RetryAfterCommitOfAnEndStatus_WritesAndAnnouncesNothingMore(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		status    domain.InstanceStatus
		eventType string
	}{
		{"completed", domain.InstanceStatusCompleted, domain.EventWorkflowInstanceFinished},
		{"failed", domain.InstanceStatusFailed, domain.EventWorkflowInstanceFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, tx := wkactNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, domain.InstanceStatusRunning)
			in := port.UpdateInstanceStatusInput{InstanceID: inst.ID.String(), TenantID: tenant.String(), Status: tc.status}

			require.NoError(t, deps.UpdateInstanceStatus(context.Background(), in))
			versionAfterFirst := world.instances[inst.ID].RecordVersion
			require.NoError(t, deps.UpdateInstanceStatus(context.Background(), in))

			assert.Equal(t, 2, tx.committed)
			assert.Len(t, world.statusWrites, 1, "the retry does not rewrite the row")
			assert.Equal(t, versionAfterFirst, world.instances[inst.ID].RecordVersion, "the retry does not bump the record version")
			assert.Len(t, world.eventsOfType(tc.eventType), 1, "one event for one transition")
			assert.Equal(t, tc.status, world.instances[inst.ID].Status)
			world.requireAllCallsForTenant(t, tenant)
		})
	}
}

func forceRouteInput(inst *domain.Instance, tenant uuid.UUID, admin, from, target string) port.RecordForceRouteInput {
	return port.RecordForceRouteInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), OldNodeKeys: []domain.NodeKey{domain.NodeKey(from)},
		TargetNodeID: target, Direction: domain.ForceRouteDirectionForward, AdminUserID: admin,
	}
}

func TestRecordForceRoute_RetryAfterCommit_AnnouncesTheRouteOnce(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	in := forceRouteInput(inst, tenant, uuid.NewString(), "sales/review", "sales/approve")

	require.NoError(t, deps.RecordForceRoute(context.Background(), in))
	require.NoError(t, deps.RecordForceRoute(context.Background(), in))

	assert.Equal(t, 2, tx.committed)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceForceRouted), 1, "a retry of the same route sends no second event")
	world.requireAllCallsForTenant(t, tenant)
}

func TestRecordForceRoute_ASecondRouteToAnotherStep_IsAnnouncedAgain(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	admin := uuid.NewString()

	require.NoError(t, deps.RecordForceRoute(context.Background(), forceRouteInput(inst, tenant, admin, "sales/review", "sales/approve")))
	require.NoError(t, deps.RecordForceRoute(context.Background(), forceRouteInput(inst, tenant, admin, "sales/approve", "sales/ship")))

	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceForceRouted), 2, "two different routes are two events")
}

func TestRecordForceRoute_AnotherInstanceWithTheSameRoute_IsAnnounced(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	first := world.addInstance(tenant, domain.InstanceStatusRunning)
	second := world.addInstance(tenant, domain.InstanceStatusRunning)
	admin := uuid.NewString()

	require.NoError(t, deps.RecordForceRoute(context.Background(), forceRouteInput(first, tenant, admin, "sales/review", "sales/approve")))
	require.NoError(t, deps.RecordForceRoute(context.Background(), forceRouteInput(second, tenant, admin, "sales/review", "sales/approve")))

	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceForceRouted), 2)
}
