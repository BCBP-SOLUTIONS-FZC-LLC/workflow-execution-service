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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTenantPurge_ATenantAnotherReplicaIsPurgingIsSkippedNotCountedAsAFailure(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	ctx := context.Background()
	tenant := uuid.New()
	seedPurgeTenant(t, superPool, tenant, 2, 2)
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-48*time.Hour), now.Add(-time.Hour)))

	failures := 0
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: repo, Data: repo, Batch: 5, Interval: time.Hour,
		Now:    func() time.Time { return now },
		Record: func(string, int64) {},
		Failed: func() { failures++ },
	})

	var tickErr error
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		var held int64
		require.NoError(t, tx.QueryRow(ctx, `SELECT record_version FROM tenant_retention WHERE tenant_id = $1 FOR SHARE`, tenant).Scan(&held))
		tickCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		tickErr = purger.Tick(tickCtx)
		return nil
	}))

	require.NoError(t, tickErr)
	assert.Zero(t, failures, "a tenant another replica holds is skipped, not failed")
	due, err := repo.ListDue(ctx, now, 10)
	require.NoError(t, err)
	assert.Equal(t, []port.TenantPurgeDue{{TenantID: tenant, Attempts: 0, RecordVersion: 1}}, due, "the row keeps its version and its attempts, and stays due")
	assert.NotZero(t, countRows(t, superPool, "workflow_instance", tenant), "nothing was purged by the skipped pass")
}
