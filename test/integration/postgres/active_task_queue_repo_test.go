//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestActiveTaskQueueRepo_RegisterGetListDeregister(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewActiveTaskQueueRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	queueName := "wf-queue-" + uuid.New().String()

	registered, err := repo.Register(ctx, tenantA, queueName)
	require.NoError(t, err)
	assert.Equal(t, queueName, registered.QueueName)
	assert.Equal(t, tenantA, registered.TenantID)

	t.Run("get by name", func(t *testing.T) {
		got, err := repo.GetByQueueName(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, registered.ID, got.ID)
	})

	t.Run("get by name for an unregistered queue is not-found", func(t *testing.T) {
		_, err := repo.GetByQueueName(ctx, "wf-queue-does-not-exist")
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("re-registering the same queue name upserts, not duplicates", func(t *testing.T) {
		reregistered, err := repo.Register(ctx, tenantA, queueName)
		require.NoError(t, err)
		assert.Equal(t, registered.ID, reregistered.ID, "conflict-on-queue_name must keep the original row's id")
	})

	t.Run("list includes the registered queue across tenants (no RLS)", func(t *testing.T) {
		queues, err := repo.ListActive(ctx)
		require.NoError(t, err)
		var found bool
		for _, q := range queues {
			if q.QueueName == queueName {
				found = true
			}
		}
		assert.True(t, found)
	})

	require.NoError(t, repo.Deregister(ctx, queueName))

	t.Run("deregistered queue is gone", func(t *testing.T) {
		_, err := repo.GetByQueueName(ctx, queueName)
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})
}
