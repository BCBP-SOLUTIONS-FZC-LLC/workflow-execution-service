//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRLSViolationRepo_CountsBetweenAndPrune(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRLSViolationRepo(pool)
	ctx := context.Background()

	insert := func(violationType string, age time.Duration) {
		t.Helper()
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx,
				`INSERT INTO rls_violation_log (violation_type, row_tenant_id, table_name, occurred_at)
				 VALUES ($1, gen_random_uuid(), 'workflow_instance', now() - $2::interval)`,
				violationType, age.String())
			return err
		}))
	}

	insert("cross_tenant_access", 0)
	start, err := repo.Horizon(ctx, 0)
	require.NoError(t, err)
	insert("cross_tenant_access", 0)
	insert("cross_tenant_access", 0)
	insert("missing_or_invalid_guc", 0)
	insert("cross_tenant_access", 10*time.Minute)

	end, err := repo.Horizon(ctx, 0)
	require.NoError(t, err)
	assert.True(t, end.After(start))

	counts, err := repo.CountsBetween(ctx, start, end)
	require.NoError(t, err)
	got := map[string]int64{}
	for _, c := range counts {
		got[c.ViolationType] = c.Count
	}
	assert.Equal(t, map[string]int64{"cross_tenant_access": 2, "missing_or_invalid_guc": 1}, got,
		"rows that occurred before the window or after its end are not counted")

	again, err := repo.CountsBetween(ctx, end, end.Add(time.Hour))
	require.NoError(t, err)
	assert.Empty(t, again)

	lagged, err := repo.Horizon(ctx, time.Minute)
	require.NoError(t, err)
	assert.WithinDuration(t, end.Add(-time.Minute), lagged, 5*time.Second)

	insert("cross_tenant_access", 48*time.Hour)
	pruned, err := repo.PruneBefore(ctx, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	assert.EqualValues(t, 1, pruned, "only the 48h-old row is pruned")
}

func TestOperationalStatsRepo_ReadsDegradedAndReadyTaskState(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewOperationalStatsRepo(pool)
	ctx := context.Background()

	empty, err := repo.Read(ctx)
	require.NoError(t, err)
	assert.Zero(t, empty.DegradedInstances)
	assert.Zero(t, empty.OldestDegradedAgeSeconds)
	assert.Zero(t, empty.OldestReadyTaskAgeSeconds)

	tenant := uuid.New()
	degradedID, runningID := uuid.New(), uuid.New()
	seedWorkflowInstance(t, pool, ctx, degradedID, tenant)
	seedWorkflowInstance(t, pool, ctx, runningID, tenant)
	taskID := uuid.New()
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if _, err := conn.Exec(ctx, `UPDATE workflow_instance SET status = 'DEGRADED', updated_at = now() - interval '2 hours' WHERE id = $1`, degradedID); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, `
			INSERT INTO workflow_task (id, tenant_id, workflow_instance_id, node_key, department_id, status, assignee_mode, created_at)
			VALUES ($1, $2, $3, 'sales/review', $4, 'READY', 'single', now() - interval '30 minutes')`,
			taskID, tenant, runningID, uuid.New())
		return err
	}))

	stats, err := repo.Read(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.DegradedInstances)
	assert.InDelta(t, 7200, stats.OldestDegradedAgeSeconds, 30)
	assert.InDelta(t, 1800, stats.OldestReadyTaskAgeSeconds, 30)
}

func TestOperationalStatsRepo_CountsNonTerminalInstancesPerQueue(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewOperationalStatsRepo(pool)
	ctx := context.Background()
	tenant := uuid.New()
	ids := map[string]uuid.UUID{}
	for _, name := range []string{"run-a1", "run-a2", "paused-a", "degraded-b", "done-a", "terminated-b"} {
		ids[name] = uuid.New()
		seedWorkflowInstance(t, pool, ctx, ids[name], tenant)
	}
	set := func(status, queue string, names ...string) {
		for _, n := range names {
			require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				_, err := conn.Exec(ctx, `UPDATE workflow_instance SET status = $2, task_queue = $3 WHERE id = $1`, ids[n], status, queue)
				return err
			}))
		}
	}
	set("RUNNING", "wf-queue-a", "run-a1", "run-a2")
	set("PAUSED", "wf-queue-a", "paused-a")
	set("DEGRADED", "wf-queue-b", "degraded-b")
	set("COMPLETED", "wf-queue-a", "done-a")
	set("TERMINATED", "wf-queue-b", "terminated-b")

	stats, err := repo.Read(ctx)
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{"wf-queue-a": 3, "wf-queue-b": 1}, stats.InstancesByQueue, "only running, paused and degraded instances count")
}

func TestRLSViolationRepo_ARowInsertedLongAfterItsTransactionStartedIsCountedInItsOwnWindow(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRLSViolationRepo(pool)
	ctx := context.Background()

	var txStart time.Time
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&txStart); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_sleep(0.3)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO rls_violation_log (violation_type, table_name) VALUES ('cross_tenant_access', 'workflow_instance')`); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}))
	end, err := repo.Horizon(ctx, 0)
	require.NoError(t, err)

	counts, err := repo.CountsBetween(ctx, txStart.Add(200*time.Millisecond), end)

	require.NoError(t, err)
	require.Len(t, counts, 1, "the row is stamped when it is written, not when its transaction began")
	assert.EqualValues(t, 1, counts[0].Count)
}
