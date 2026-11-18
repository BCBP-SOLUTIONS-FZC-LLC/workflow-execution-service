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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func w8bSuperExec(t *testing.T, superPool *pgcommon.Pool, sql string, args ...any) {
	t.Helper()
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, sql, args...)
		return err
	}))
}

func TestTenantRetentionRepo_PurgeBatch_WhenAStepFails_NamesTheTableAndDeletesNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(superPool).Create(ctx, inst))
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	require.NoError(t, repo.RecordOffboarded(ctx, tenantID, now.Add(-48*time.Hour), now.Add(-time.Hour)))
	due, err := repo.ListDue(ctx, now, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	version := due[0].RecordVersion

	w8bSuperExec(t, superPool, `CREATE TABLE w8b_instance_pin (instance_id uuid NOT NULL REFERENCES workflow_instance (id))`)
	w8bSuperExec(t, superPool, `INSERT INTO w8b_instance_pin (instance_id) VALUES ($1)`, inst.ID)

	deleted, err := repo.PurgeBatch(ctx, tenantID, version, 10)

	require.ErrorContains(t, err, "purge batch: purge workflow_instance")
	assert.Nil(t, deleted)
	assert.Equal(t, 1, countRows(t, superPool, "workflow_instance", tenantID), "the failed batch deleted nothing")

	w8bSuperExec(t, superPool, `DELETE FROM w8b_instance_pin`)
	deleted, err = repo.PurgeBatch(ctx, tenantID, version, 10)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"workflow_instance": 1}, deleted, "once the blocking row is gone the same batch deletes the instance")
}
