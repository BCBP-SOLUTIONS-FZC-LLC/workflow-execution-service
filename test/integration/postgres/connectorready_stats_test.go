//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// The age of the oldest connector task still READY counts connector tasks only: a human task that waits longer, a
// connector task that is no longer READY and a connector task of another age do not move it.
func TestOperationalStatsRepo_ReadsOldestReadyConnectorTaskAge(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewOperationalStatsRepo(pool)
	ctx := context.Background()

	empty, err := repo.Read(ctx)
	require.NoError(t, err)
	assert.Zero(t, empty.OldestReadyConnectorTaskAgeSeconds)

	tenant, instanceID := uuid.New(), uuid.New()
	seedWorkflowInstance(t, pool, ctx, instanceID, tenant)
	insert := func(status string, connectorType *string, ageMinutes int) {
		t.Helper()
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, `
				INSERT INTO workflow_task (id, tenant_id, workflow_instance_id, node_key, department_id, status, assignee_mode, connector_type, created_at)
				VALUES ($1, $2, $3, 'stage/'||gen_random_uuid()::text, $4, $5, 'single', $6, now() - make_interval(mins => $7))`,
				uuid.New(), tenant, instanceID, uuid.New(), status, connectorType, ageMinutes)
			return err
		}))
	}
	email := "send_email"

	insert("READY", nil, 180)
	insert("COMPLETED", &email, 300)
	insert("READY", &email, 45)
	insert("READY", &email, 10)

	stats, err := repo.Read(ctx)
	require.NoError(t, err)
	assert.InDelta(t, 45*60, stats.OldestReadyConnectorTaskAgeSeconds, 30)
	assert.InDelta(t, 180*60, stats.OldestReadyTaskAgeSeconds, 30, "the all-tasks gauge keeps counting every READY task")
}
