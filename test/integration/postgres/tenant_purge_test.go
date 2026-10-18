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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var purgeTables = []string{
	"assignee_overrides", "workflow_task_assignment", "workflow_task", "workflow_instance",
	"connector_documents", "connector_document_attempts", "connector_send_intents",
	"active_task_queues", "event_recency_guard",
}

func countRows(t *testing.T, pool *pgcommon.Pool, table string, tenantID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		switch table {
		case "event_recency_guard":
			return conn.QueryRow(ctx, `SELECT count(*) FROM event_recency_guard WHERE scope_key = $1 OR scope_key LIKE $2`,
				"tenant:"+tenantID.String(), "user_availability:"+tenantID.String()+":%").Scan(&n)
		case "connector_documents", "connector_document_attempts", "connector_send_intents":
			return conn.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE tenant_id = $1`, tenantID.String()).Scan(&n)
		default:
			return conn.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE tenant_id = $1`, tenantID).Scan(&n)
		}
	}))
	return n
}

func seedPurgeTenant(t *testing.T, superPool *pgcommon.Pool, tenantID uuid.UUID, instances, tasksPerInstance int) {
	t.Helper()
	ctx := context.Background()
	instanceRepo := postgres.NewInstanceRepo(superPool)
	taskRepo := postgres.NewTaskRepo(superPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(superPool)
	overrideRepo := postgres.NewAssigneeOverrideRepo(superPool)
	for i := 0; i < instances; i++ {
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		var previous *domain.Task
		for j := 0; j < tasksPerInstance; j++ {
			task := newTask(tenantID, inst.ID)
			if previous != nil {
				task.DeferredFromTaskID = &previous.ID
			}
			require.NoError(t, taskRepo.Create(ctx, task))
			require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantID, task.ID, uuid.New())))
			previous = task
		}
		require.NoError(t, overrideRepo.Create(ctx, &domain.AssigneeOverride{
			ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
			PreviousUserID: uuid.New(), NewUserID: uuid.New(), Reason: "leave", ActorUserID: uuid.New(),
		}))
	}
	_, err := postgres.NewActiveTaskQueueRepo(superPool).Register(ctx, tenantID, domain.TenantTaskQueueName(tenantID))
	require.NoError(t, err)
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		stmts := []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO tenant_state (tenant_id, status, changed_at) VALUES ($1, 'offboarded', now())`, []any{tenantID}},
			{`INSERT INTO event_recency_guard (scope_key, last_applied_at) VALUES ($1, now())`, []any{"tenant:" + tenantID.String()}},
			{`INSERT INTO event_recency_guard (scope_key, last_applied_at) VALUES ($1, now())`, []any{"user_availability:" + tenantID.String() + ":" + uuid.NewString()}},
			{`INSERT INTO connector_documents (tenant_id, provider, container, filename, state) VALUES ($1, 'drive', 'c', 'f', 'AVAILABLE')`, []any{tenantID.String()}},
			{`INSERT INTO connector_document_attempts (document_id, tenant_id, attempt, outcome) VALUES (gen_random_uuid(), $1, 'a1', 'available')`, []any{tenantID.String()}},
			{`INSERT INTO connector_send_intents (tenant_id, message_key, status) VALUES ($1, 'm1', 'accepted')`, []any{tenantID.String()}},
		}
		for _, s := range stmts {
			if _, err := conn.Exec(ctx, s.sql, s.args...); err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestTenantPurge_AsTheTenantUnderRLSDeletesADueTenantChildFirstAndLeavesEveryoneElse(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()
	repo := postgres.NewTenantRetentionRepo(appPool)

	due, notDue, neverOffboarded := uuid.New(), uuid.New(), uuid.New()
	for _, tenant := range []uuid.UUID{due, notDue, neverOffboarded} {
		seedPurgeTenant(t, superPool, tenant, 3, 4)
	}
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	require.NoError(t, repo.RecordOffboarded(ctx, due, now.Add(-48*time.Hour), now.Add(-time.Hour)))
	require.NoError(t, repo.RecordOffboarded(ctx, notDue, now.Add(-48*time.Hour), now.Add(24*time.Hour)))

	recorded := map[string]int64{}
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: repo, Data: repo, Batch: 5, Interval: time.Hour,
		Now:    func() time.Time { return now },
		Record: func(table string, n int64) { recorded[table] += n },
		Failed: func() {},
		Logger: nil,
	})

	has, err := repo.HasRows(ctx, due)
	require.NoError(t, err)
	assert.True(t, has, "the seeded tenant has rows")

	require.NoError(t, purger.Tick(ctx))

	has, err = repo.HasRows(ctx, due)
	require.NoError(t, err)
	assert.False(t, has, "nothing is left of the purged tenant")
	has, err = repo.HasRows(ctx, notDue)
	require.NoError(t, err)
	assert.True(t, has, "another tenant's rows are not counted")

	for _, table := range purgeTables {
		assert.Zero(t, countRows(t, superPool, table, due), "%s rows of the due tenant", table)
		assert.NotZero(t, countRows(t, superPool, table, notDue), "%s rows of a tenant whose period has not passed", table)
		assert.NotZero(t, countRows(t, superPool, table, neverOffboarded), "%s rows of a tenant never offboarded", table)
	}
	assert.EqualValues(t, 3, recorded["workflow_instance"])
	assert.EqualValues(t, 12, recorded["workflow_task"])

	assert.Equal(t, 1, countRows(t, superPool, "tenant_state", due), "the recorded status stays as a marker, so the start gate still sees an offboarded tenant after its rows are gone")

	var purgedAt *time.Time
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT purged_at FROM tenant_retention WHERE tenant_id = $1`, due).Scan(&purgedAt)
	}))
	require.NotNil(t, purgedAt)

	require.NoError(t, purger.Tick(ctx), "a second run finds nothing due")
}

func TestTenantPurge_LeavesTheAuditTrailAlone(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()
	repo := postgres.NewTenantRetentionRepo(appPool)
	tenant := uuid.New()
	seedPurgeTenant(t, superPool, tenant, 1, 1)
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events (id, event_type, payload, tenant_id) VALUES ($1, 'WorkflowInstanceStarted', '{}', $2)`, uuid.New(), tenant.String())
		return err
	}))
	now := time.Now().UTC()
	require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-time.Hour), now.Add(-time.Minute)))

	require.NoError(t, service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: repo, Data: repo, Batch: 100, Interval: time.Hour, Now: func() time.Time { return now },
		Record: func(string, int64) {}, Failed: func() {},
	}).Tick(ctx))

	var events int
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tenant_id = $1`, tenant.String()).Scan(&events)
	}))
	assert.Equal(t, 1, events, "the outbox is the audit trail and keeps its own retention")
}

func TestTenantRetentionRepo_ARedeliveredOffboardingKeepsTheFirstDates(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewTenantRetentionRepo(pool)
	ctx := context.Background()
	tenant := uuid.New()
	first := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	require.NoError(t, repo.RecordOffboarded(ctx, tenant, first, first.Add(24*time.Hour)))
	require.NoError(t, repo.RecordOffboarded(ctx, tenant, first.Add(72*time.Hour), first.Add(96*time.Hour)))

	due, err := repo.ListDue(ctx, first.Add(25*time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{tenant}, due, "the first purge date still applies")
}

func TestTenantStateRepo_ARecordedStatusIsReadBackAndAnOlderChangeNeverOverwritesANewerOne(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewTenantStateRepo(pool)
	ctx := context.Background()
	tenant := uuid.New()
	t0 := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

	_, found, err := repo.Get(ctx, tenant)
	require.NoError(t, err)
	assert.False(t, found, "a tenant with no event yet has no status")

	require.NoError(t, repo.Record(ctx, tenant, "suspended", t0))
	require.NoError(t, repo.Record(ctx, tenant, "active", t0.Add(time.Hour)))
	require.NoError(t, repo.Record(ctx, tenant, "suspended", t0.Add(-time.Hour)))

	status, found, err := repo.Get(ctx, tenant)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "active", status, "a delayed older event must not undo the newer status")
}
