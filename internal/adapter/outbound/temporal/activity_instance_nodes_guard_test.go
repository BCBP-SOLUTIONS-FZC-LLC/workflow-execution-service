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

func TestUpdateInstanceNodes_AnEndedInstance_KeepsItsNodesAndItsVersion(t *testing.T) {
	t.Parallel()

	for _, status := range []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusFailed, domain.InstanceStatusTerminated} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, _ := wkactNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, status)
			inst.CurrentNodeKeys = []string{"sales/approve"}

			err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
				InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKeys: []domain.NodeKey{"sales/review"},
			})

			require.NoError(t, err, "a late call is answered as done, not retried")
			assert.Empty(t, world.nodeWrites)
			assert.Equal(t, []string{"sales/approve"}, world.instances[inst.ID].CurrentNodeKeys)
			assert.Equal(t, int64(1), world.instances[inst.ID].RecordVersion)
		})
	}
}

func TestUpdateInstanceNodes_SameNodesAgain_WritesNothingAndBumpsTheVersionOnce(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	in := port.UpdateInstanceNodesInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKeys: []domain.NodeKey{"sales/review", "sales/approve"},
	}

	require.NoError(t, deps.UpdateInstanceNodes(context.Background(), in))
	require.NoError(t, deps.UpdateInstanceNodes(context.Background(), in))

	assert.Len(t, world.nodeWrites, 1, "the retry writes nothing")
	assert.Equal(t, int64(2), world.instances[inst.ID].RecordVersion, "one bump from version 1")
	assert.Equal(t, []string{"sales/review", "sales/approve"}, world.instances[inst.ID].CurrentNodeKeys)
}

func TestUpdateInstanceNodes_ReadAndWriteShareOneTransactionForTheTenant(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)

	err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKeys: []domain.NodeKey{"sales/review"},
	})

	require.NoError(t, err)
	require.Len(t, tx.opened, 1)
	assert.True(t, tx.opened[0].GUCSet)
	assert.Equal(t, tenant.String(), tx.opened[0].Tenant)
	assert.Equal(t, 1, tx.committed)
	assert.Len(t, world.callsTo("Instances.GetByID"), 1)
	assert.Len(t, world.nodeWrites, 1)
}

func TestUpdateInstanceNodes_AFailedWriteRollsBackTheTransaction(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	inst.RecordVersion = 6
	deps.Instances = staleVersionInstances{wkactInstances{w: world}, 5}

	err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKeys: []domain.NodeKey{"sales/review"},
	})

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	assert.Equal(t, 1, tx.rolledBack, "the failure goes through the transactor")
}

// staleVersionInstances hands out the instance as it was at an older record
// version, so a write built from it conflicts.
type staleVersionInstances struct {
	wkactInstances
	version int64
}

func (r staleVersionInstances) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.Instance, error) {
	inst, err := r.wkactInstances.GetByID(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	stale := *inst
	stale.RecordVersion = r.version
	return &stale, nil
}
