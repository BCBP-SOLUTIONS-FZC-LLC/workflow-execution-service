//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func w8bRequireNoClientAnswer(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	for _, answer := range []error{
		domain.ErrNotFound, domain.ErrRecordVersionConflict, domain.ErrAlreadyExists,
		domain.ErrDuplicateActiveAssignment, port.ErrTenantRetentionChanged,
	} {
		assert.NotErrorIs(t, err, answer, "a database failure must not look like a client answer")
	}
}

func w8bDropTable(t *testing.T, superPool *pgcommon.Pool, table string) {
	t.Helper()
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, "DROP TABLE "+table+" CASCADE")
		return err
	}))
}

func TestEndedDelegationRepo_DelegateMethods_WhenThePoolIsClosed_ReturnTheOutageAndNoAnswer(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ended := postgres.NewEndedDelegationRepo(appPool)
	appPool.Close()
	ctx := context.Background()
	tenantID, delegateID := uuid.New(), uuid.New()

	t.Run("marking a delegate ended", func(t *testing.T) {
		err := ended.MarkDelegateEnded(ctx, tenantID, delegateID, time.Now().UTC())
		w8bRequireNoClientAnswer(t, err)
		assert.True(t, pgcommon.IsPoolClosed(err), "the pool's own error reaches the caller: %v", err)
	})
	t.Run("reading when a delegate ended", func(t *testing.T) {
		endedAt, found, err := ended.DelegateEndedAt(ctx, tenantID, delegateID)
		w8bRequireNoClientAnswer(t, err)
		assert.True(t, pgcommon.IsPoolClosed(err), "the pool's own error reaches the caller: %v", err)
		assert.False(t, found, "an outage must not read as a delegate that never ended")
		assert.True(t, endedAt.IsZero())
	})
}

func TestEndedDelegationRepo_DelegateMethods_WhenTheStatementFails_ReturnTheDatabaseErrorAndNoAnswer(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ended := postgres.NewEndedDelegationRepo(appPool)
	ctx := context.Background()
	tenantID, delegateID := uuid.New(), uuid.New()
	endedAt := time.Now().UTC().Truncate(time.Microsecond)

	require.NoError(t, ended.MarkDelegateEnded(ctx, tenantID, delegateID, endedAt))
	got, found, err := ended.DelegateEndedAt(ctx, tenantID, delegateID)
	require.NoError(t, err)
	require.True(t, found, "the same read finds the delegate while the table exists")
	require.True(t, endedAt.Equal(got))

	w8bDropTable(t, superPool, "ended_delegate")

	t.Run("marking a delegate ended", func(t *testing.T) {
		err := ended.MarkDelegateEnded(ctx, tenantID, delegateID, endedAt)
		w8bRequireNoClientAnswer(t, err)
		assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
	})
	t.Run("reading when a delegate ended", func(t *testing.T) {
		gotAt, gotFound, err := ended.DelegateEndedAt(ctx, tenantID, delegateID)
		w8bRequireNoClientAnswer(t, err)
		assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
		assert.False(t, gotFound, "a failed read must not report an ended delegate")
		assert.True(t, gotAt.IsZero())
	})
}

type w8bReader struct {
	name string
	call func(ctx context.Context, pool *pgcommon.Pool) (any, error)
}

func w8bReaders() []w8bReader {
	return []w8bReader{
		{"active task queues", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewActiveTaskQueueRepo(pool).ListActive(ctx)
		}},
		{"assignee overrides by instance", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewAssigneeOverrideRepo(pool).ListByInstance(ctx, uuid.New(), uuid.New())
		}},
		{"outbox record for a task", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewOutboxRepo(pool).ExistsForTask(ctx, "workflow.task.sla_warning", uuid.New())
		}},
		{"row security violation horizon", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewRLSViolationRepo(pool).Horizon(ctx, time.Minute)
		}},
		{"row security violation prune", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewRLSViolationRepo(pool).PruneBefore(ctx, time.Now())
		}},
		{"purges due list", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewTenantRetentionRepo(pool).ListDue(ctx, time.Now(), 10)
		}},
		{"tenant rows probe", func(ctx context.Context, pool *pgcommon.Pool) (any, error) {
			return postgres.NewTenantRetentionRepo(pool).HasRows(ctx, uuid.New())
		}},
	}
}

func TestRepositories_ReadAndListPaths_WhenTheContextIsCancelled_ReturnTheErrorAndNoResult(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range w8bReaders() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.call(cancelled, appPool)

			w8bRequireNoClientAnswer(t, err)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Empty(t, got)
		})
	}
}

func TestRepositories_ReadAndListPaths_WhenThePoolIsClosed_ReturnTheOutageAndNoResult(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	appPool.Close()

	for _, tc := range w8bReaders() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.call(context.Background(), appPool)

			w8bRequireNoClientAnswer(t, err)
			assert.True(t, pgcommon.IsPoolClosed(err), "the pool's own error reaches the caller: %v", err)
			assert.Empty(t, got)
		})
	}
}

const w8bStatsRolePassword = "statspassword123"

func w8bNewStatsRolePool(t *testing.T, superPool *pgcommon.Pool, superDSN string) *pgcommon.Pool {
	t.Helper()
	ctx := context.Background()
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE w8b_stats LOGIN BYPASSRLS PASSWORD '%s'", w8bStatsRolePassword),
		"GRANT CONNECT ON DATABASE testdb TO w8b_stats",
		"GRANT USAGE ON SCHEMA public TO w8b_stats",
		"GRANT SELECT ON workflow_instance, workflow_task TO w8b_stats",
	} {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, stmt)
			return err
		}))
	}
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: fmt.Sprintf("postgres://w8b_stats:%s@%s", w8bStatsRolePassword, hostAndDB(superDSN))})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestOperationalStatsRepo_Read_WhenTheSecondQueryFails_ReturnsTheErrorAndZeroStats(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	statsPool := w8bNewStatsRolePool(t, superPool, superDSN)
	stats := postgres.NewOperationalStatsRepo(statsPool)
	ctx := context.Background()

	tenantID, degradedID := uuid.New(), uuid.New()
	seedWorkflowInstance(t, superPool, ctx, degradedID, tenantID)
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE workflow_instance SET status = 'DEGRADED', updated_at = now() - interval '2 hours' WHERE id = $1`, degradedID)
		return err
	}))

	healthy, err := stats.Read(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, healthy.DegradedInstances, "the first query has something to report")
	require.Equal(t, map[string]int64{"execution-default": 1}, healthy.InstancesByQueue)

	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if _, err := conn.Exec(ctx, "REVOKE SELECT ON workflow_instance FROM w8b_stats"); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, "GRANT SELECT (id, tenant_id, status, updated_at) ON workflow_instance TO w8b_stats")
		return err
	}))

	got, err := stats.Read(ctx)

	w8bRequireNoClientAnswer(t, err)
	assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
	assert.Equal(t, domain.OperationalStats{}, got, "nothing from the first query leaks into the result")
}
