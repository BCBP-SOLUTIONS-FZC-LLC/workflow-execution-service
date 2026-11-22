//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// requireOutage fails unless err is the database being unreachable: an error that is
// none of the domain answers a caller would act on as "not found" or "stale".
func requireOutage(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	for _, answer := range []error{
		domain.ErrNotFound, domain.ErrRecordVersionConflict, domain.ErrDuplicateActiveAssignment,
		port.ErrTenantRetentionChanged,
	} {
		assert.NotErrorIs(t, err, answer, "an outage must not look like a client error")
	}
	assert.Contains(t, err.Error(), "connection pool is closed")
}

func TestRepositories_ClosedPool_ReturnTheOutageUnmapped(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ended := postgres.NewEndedDelegationRepo(appPool)
	recency := postgres.NewRecencyGuardRepo(appPool, appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	retention := postgres.NewTenantRetentionRepo(appPool)
	ctx := context.Background()
	tenantID, id := uuid.New(), uuid.New()
	appPool.Close()

	t.Run("ended delegation read", func(t *testing.T) {
		found, err := ended.IsEnded(ctx, tenantID, id)
		requireOutage(t, err)
		assert.False(t, found, "an outage must not read as an ended delegation")
	})
	t.Run("ended delegation write", func(t *testing.T) {
		requireOutage(t, ended.MarkEnded(ctx, tenantID, id))
	})
	t.Run("recency check", func(t *testing.T) {
		apply, err := recency.ShouldApply(ctx, "scope", time.Now())
		requireOutage(t, err)
		assert.False(t, apply)
	})
	t.Run("recency check that lets an equal time through", func(t *testing.T) {
		apply, err := recency.ShouldApplyUnlessOlder(ctx, "scope", time.Now())
		requireOutage(t, err)
		assert.False(t, apply)
	})
	t.Run("assignments by reason tag", func(t *testing.T) {
		found, err := assignments.ListActiveByReasonTag(ctx, tenantID, "delegation:"+id.String())
		requireOutage(t, err)
		assert.Empty(t, found)
	})
	t.Run("claim", func(t *testing.T) {
		claimed, err := assignments.ClaimLead(ctx, tenantID, uuid.New(), id, 1)
		requireOutage(t, err)
		assert.Nil(t, claimed)
	})
	t.Run("lead move", func(t *testing.T) {
		moved, err := assignments.SetLead(ctx, tenantID, uuid.New(), id, 1)
		requireOutage(t, err)
		assert.Nil(t, moved)
	})
	t.Run("tenant rows probe", func(t *testing.T) {
		has, err := retention.HasRows(ctx, tenantID)
		requireOutage(t, err)
		assert.False(t, has)
	})
	t.Run("purges due list", func(t *testing.T) {
		due, err := retention.ListDue(ctx, time.Now(), 10)
		requireOutage(t, err)
		assert.Empty(t, due)
	})
}

const undefinedTable = "42P01"

func requireQueryFailure(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "the database's own error reaches the caller")
	assert.Equal(t, undefinedTable, pgErr.Code)
	for _, answer := range []error{domain.ErrNotFound, domain.ErrRecordVersionConflict, domain.ErrAlreadyExists} {
		assert.NotErrorIs(t, err, answer, "a failed statement must not look like a client error")
	}
}

func dropTable(t *testing.T, superPool *pgcommon.Pool, table string) {
	t.Helper()
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, "DROP TABLE "+table+" CASCADE")
		return err
	}))
}

func TestRepositories_AFailingStatement_ReturnsTheDatabaseErrorWithNoResult(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ended := postgres.NewEndedDelegationRepo(appPool)
	recency := postgres.NewRecencyGuardRepo(appPool, appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	retention := postgres.NewTenantRetentionRepo(appPool)
	ctx := context.Background()
	tenantID, id := uuid.New(), uuid.New()

	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, postgres.NewTaskRepo(appPool).Create(ctx, task))

	t.Run("a lead move of an assignment that does not exist is not found", func(t *testing.T) {
		_, err := assignments.ClaimLead(ctx, tenantID, task.ID, id, task.RecordVersion)
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, err = assignments.SetLead(ctx, tenantID, task.ID, id, task.RecordVersion)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	dropTable(t, superPool, "ended_delegation")
	t.Run("ended delegation read", func(t *testing.T) {
		found, err := ended.IsEnded(ctx, tenantID, id)
		requireQueryFailure(t, err)
		assert.False(t, found, "a failed read must not report an ended delegation")
	})
	t.Run("ended delegation write", func(t *testing.T) {
		requireQueryFailure(t, ended.MarkEnded(ctx, tenantID, id))
	})
	t.Run("tenant rows probe", func(t *testing.T) {
		has, err := retention.HasRows(ctx, tenantID)
		requireQueryFailure(t, err)
		assert.False(t, has)
	})

	dropTable(t, superPool, "event_recency_guard")
	t.Run("recency check", func(t *testing.T) {
		apply, err := recency.ShouldApply(ctx, "scope", time.Now())
		requireQueryFailure(t, err)
		assert.False(t, apply)
	})
	t.Run("recency check that lets an equal time through", func(t *testing.T) {
		apply, err := recency.ShouldApplyUnlessOlder(ctx, "scope", time.Now())
		requireQueryFailure(t, err)
		assert.False(t, apply)
	})

	dropTable(t, superPool, "tenant_retention")
	t.Run("purges due list", func(t *testing.T) {
		due, err := retention.ListDue(ctx, time.Now(), 10)
		requireQueryFailure(t, err)
		assert.Empty(t, due)
	})

	dropTable(t, superPool, "workflow_task_assignment")
	t.Run("assignments by reason tag", func(t *testing.T) {
		found, err := assignments.ListActiveByReasonTag(ctx, tenantID, "delegation:"+id.String())
		requireQueryFailure(t, err)
		assert.Empty(t, found)
	})
	t.Run("a lead move whose lead clearing fails", func(t *testing.T) {
		claimed, err := assignments.ClaimLead(ctx, tenantID, task.ID, id, task.RecordVersion)
		requireQueryFailure(t, err)
		assert.Nil(t, claimed)
	})
}
