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

// A READY connector task of an instance that is paused or already ended is not stuck: nothing is meant to run it. Only
// instances in RUNNING or DEGRADED count toward the gauge.
func TestOperationalStatsRepo_OldestReadyConnectorTaskIgnoresPausedAndEndedInstances(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewOperationalStatsRepo(pool)
	ctx := context.Background()
	tenant := uuid.New()
	email := "send_email"

	readyConnectorTaskOnInstance := func(status string, ageMinutes int) {
		t.Helper()
		instanceID := uuid.New()
		seedWorkflowInstance(t, pool, ctx, instanceID, tenant)
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			if _, err := conn.Exec(ctx, `UPDATE workflow_instance SET status = $2 WHERE id = $1`, instanceID, status); err != nil {
				return err
			}
			_, err := conn.Exec(ctx, `
				INSERT INTO workflow_task (id, tenant_id, workflow_instance_id, node_key, department_id, status, assignee_mode, connector_type, created_at)
				VALUES ($1, $2, $3, 'stage/connector', $4, 'READY', 'single', $5, now() - make_interval(mins => $6))`,
				uuid.New(), tenant, instanceID, uuid.New(), email, ageMinutes)
			return err
		}))
	}
	oldestConnectorAge := func() float64 {
		t.Helper()
		stats, err := repo.Read(ctx)
		require.NoError(t, err)
		return stats.OldestReadyConnectorTaskAgeSeconds
	}

	readyConnectorTaskOnInstance("PAUSED", 500)
	readyConnectorTaskOnInstance("COMPLETED", 400)
	readyConnectorTaskOnInstance("TERMINATED", 300)
	readyConnectorTaskOnInstance("FAILED", 200)
	assert.Zero(t, oldestConnectorAge(), "tasks of paused or ended instances are not stuck")

	readyConnectorTaskOnInstance("RUNNING", 45)
	assert.InDelta(t, 45*60, oldestConnectorAge(), 30, "a task of a running instance counts")

	readyConnectorTaskOnInstance("DEGRADED", 60)
	assert.InDelta(t, 60*60, oldestConnectorAge(), 30, "a task of a degraded instance counts")
}
