//go:build integration

package postgres_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRecencyGuardRepo_CheckAndCommit_AnEqualTimestamp_IsNotAppliedAndChangesNothing(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "user_availability:" + uuid.NewString()
	at := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

	applied, err := repo.CheckAndCommit(ctx, scopeKey, at)
	require.NoError(t, err)
	require.True(t, applied)

	readRow := func() (lastApplied, updatedAt time.Time) {
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, `SELECT last_applied_at, updated_at FROM event_recency_guard WHERE scope_key = $1`, scopeKey).Scan(&lastApplied, &updatedAt)
		}))
		return lastApplied, updatedAt
	}
	_, updatedBefore := readRow()

	applied, err = repo.CheckAndCommit(ctx, scopeKey, at)
	require.NoError(t, err, "an event that does not apply is not an error")
	assert.False(t, applied, "an equal timestamp is a duplicate, not newer")

	lastApplied, updatedAfter := readRow()
	assert.True(t, lastApplied.Equal(at))
	assert.True(t, updatedAfter.Equal(updatedBefore), "a skipped event does not touch the row")

	again, err := repo.CheckAndCommit(ctx, scopeKey, at.Add(time.Microsecond))
	require.NoError(t, err)
	assert.True(t, again, "one microsecond later is newer and applies")
}

func TestUserAvailabilityRepo_Record_AnEqualTimestampOverwritesAndEveryWriteMovesTheVersionByOne(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewUserAvailabilityRepo(newAppRolePool(t, superPool, superDSN))
	ctx := context.Background()
	tenant, user := uuid.New(), uuid.New()
	at := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

	stored := func() (status string, version int64) {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, `SELECT status::text, record_version FROM user_availability WHERE tenant_id = $1 AND user_id = $2`, tenant, user).Scan(&status, &version)
		}))
		return status, version
	}

	steps := []struct {
		name        string
		status      string
		changedAt   time.Time
		wantWritten bool
		wantStatus  string
		wantVersion int64
	}{
		{name: "the first change inserts the row at version 1", status: "available", changedAt: at, wantWritten: true, wantStatus: "available", wantVersion: 1},
		{name: "an older change is not written and the version stays", status: "ooo", changedAt: at.Add(-time.Second), wantWritten: false, wantStatus: "available", wantVersion: 1},
		{name: "a newer change writes and moves the version", status: "busy", changedAt: at.Add(time.Minute), wantWritten: true, wantStatus: "busy", wantVersion: 2},
		{name: "an older change after that still changes nothing", status: "available", changedAt: at, wantWritten: false, wantStatus: "busy", wantVersion: 2},
	}
	for _, step := range steps {
		written, err := repo.Record(ctx, tenant, user, step.status, step.changedAt)
		require.NoError(t, err, step.name)
		assert.Equal(t, step.wantWritten, written, step.name)
		status, version := stored()
		assert.Equal(t, step.wantStatus, status, step.name)
		assert.Equal(t, step.wantVersion, version, step.name)
		got, found, err := repo.Get(ctx, tenant, user)
		require.NoError(t, err, step.name)
		assert.True(t, found, step.name)
		assert.Equal(t, step.wantStatus, got, step.name)
	}
}

func TestActiveTaskQueueRepo_ListActive_ReturnsQueuesOrderedByName(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	repo := postgres.NewActiveTaskQueueRepo(newAppRolePool(t, superPool, superDSN))
	ctx := context.Background()

	registeredInOrder := []string{"wf-queue-c", "wf-queue-a", "wf-queue-d", "wf-queue-b"}
	for _, name := range registeredInOrder {
		_, err := repo.Register(ctx, uuid.New(), name)
		require.NoError(t, err)
	}
	require.NoError(t, repo.StopRouting(ctx, "wf-queue-a"))

	queues, err := repo.ListActive(ctx)
	require.NoError(t, err)
	var got []string
	for _, q := range queues {
		got = append(got, q.QueueName)
	}
	want := append([]string(nil), registeredInOrder...)
	sort.Strings(want)
	assert.Equal(t, want, got, "queue_name ascending, a queue that stopped routing included")
}

func TestInstanceRepo_Create_InsideATransaction_ReturnsTheServerSetFieldsAndFreesTheKeyOnceTheInstanceEnded(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()
	tenant := uuid.New()

	inst := newInstance(tenant, time.Now().UTC().Add(-time.Hour))
	inst.CurrentNodeKeys = []string{"review", "approve"}
	require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(txCtx context.Context) error {
		return repo.Create(txCtx, inst)
	}))

	t.Run("the returned row carries what the server set", func(t *testing.T) {
		assert.Equal(t, int64(1), inst.RecordVersion)
		assert.False(t, inst.CreatedAt.IsZero())
		assert.True(t, inst.UpdatedAt.Equal(inst.CreatedAt), "both are set by the same statement")
		assert.True(t, inst.CreatedAt.Equal(w24bStoredCreatedAt(t, superPool, "workflow_instance", inst.ID)), "the returned created_at is the stored one")
		assert.NotNil(t, inst.SavedNodeKeys, "saved_node_keys comes back as an empty array, not nil")
		assert.Empty(t, inst.SavedNodeKeys)
		assert.Nil(t, inst.CompletedAt)
		assert.Equal(t, []string{"review", "approve"}, inst.CurrentNodeKeys)
	})

	t.Run("the row is committed and readable by its tenant", func(t *testing.T) {
		got, err := repo.GetByID(ctx, tenant, inst.ID)
		require.NoError(t, err)
		assert.Equal(t, inst.BusinessKey, got.BusinessKey)
		assert.Empty(t, got.SavedNodeKeys)
		assert.Nil(t, got.CompletedAt)
	})

	t.Run("the same subject and live business key again is a duplicate and the transaction rolls back", func(t *testing.T) {
		dup := newInstance(tenant, time.Now().UTC())
		dup.SubjectID, dup.BusinessKey = inst.SubjectID, inst.BusinessKey
		err := transactor.RunInTx(withGUC(ctx, tenant), func(txCtx context.Context) error {
			return repo.Create(txCtx, dup)
		})
		require.ErrorIs(t, err, domain.ErrDuplicateBusinessKey)
		_, getErr := repo.GetByID(ctx, tenant, dup.ID)
		require.ErrorIs(t, getErr, domain.ErrNotFound)
	})

	t.Run("once the first instance is completed the key can be used again", func(t *testing.T) {
		_, err := repo.UpdateStatus(ctx, tenant, inst.ID, domain.InstanceStatusCompleted, inst.RecordVersion)
		require.NoError(t, err)

		reuse := newInstance(tenant, time.Now().UTC())
		reuse.SubjectID, reuse.BusinessKey = inst.SubjectID, inst.BusinessKey
		require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(txCtx context.Context) error {
			return repo.Create(txCtx, reuse)
		}))
		live, err := repo.GetLiveByBusinessKey(ctx, tenant, inst.SubjectID, inst.BusinessKey)
		require.NoError(t, err)
		assert.Equal(t, reuse.ID, live.ID, "the new instance is the live holder of the key")
	})
}

func TestAssigneeOverrideRepo_ListByInstance_NewestFirstAndOnlyForTheCallersTenant(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	overrides := postgres.NewAssigneeOverrideRepo(appPool)
	ctx := context.Background()
	owner, intruder := uuid.New(), uuid.New()

	inst := newInstance(owner, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)

	var seeded []uuid.UUID
	for i, reason := range []string{"first", "", "third"} {
		override := &domain.AssigneeOverride{
			ID: uuid.New(), TenantID: owner, WorkflowInstanceID: inst.ID, NodeKey: "finance/review",
			NewUserID: uuid.New(), Reason: reason, ActorUserID: uuid.New(),
		}
		require.NoError(t, overrides.Create(ctx, override))
		w8bSuperExec(t, superPool, "UPDATE assignee_overrides SET created_at = $2 WHERE id = $1", override.ID, base.Add(time.Duration(i)*time.Minute))
		seeded = append(seeded, override.ID)
	}

	cases := []struct {
		name     string
		tenant   uuid.UUID
		instance uuid.UUID
		want     []uuid.UUID
	}{
		{name: "the owner gets every override newest first", tenant: owner, instance: inst.ID, want: []uuid.UUID{seeded[2], seeded[1], seeded[0]}},
		{name: "another tenant naming the same instance gets an empty list", tenant: intruder, instance: inst.ID, want: nil},
		{name: "an instance with no overrides gets an empty list", tenant: owner, instance: uuid.New(), want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := overrides.ListByInstance(ctx, tc.tenant, tc.instance)
			require.NoError(t, err)
			assert.NotNil(t, got)
			var ids []uuid.UUID
			for _, o := range got {
				ids = append(ids, o.ID)
			}
			assert.Equal(t, tc.want, ids)
		})
	}

	t.Run("an override with no reason is stored as null and read back as empty", func(t *testing.T) {
		var isNull bool
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, `SELECT reason IS NULL FROM assignee_overrides WHERE id = $1`, seeded[1]).Scan(&isNull)
		}))
		assert.True(t, isNull)
		got, err := overrides.ListByInstance(ctx, owner, inst.ID)
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, "", got[1].Reason)
		assert.Equal(t, "third", got[0].Reason)
		assert.Equal(t, "first", got[2].Reason)
	})
}

func TestRepos_CreatingARowUnderAnotherTenantsParent_IsRefusedAndWritesNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	overrides := postgres.NewAssigneeOverrideRepo(appPool)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()

	instA, instB := newInstance(tenantA, time.Now().UTC()), newInstance(tenantB, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, instA))
	require.NoError(t, instances.Create(ctx, instB))
	taskA, taskB := newTask(tenantA, instA.ID), newTask(tenantB, instB.ID)
	require.NoError(t, tasks.Create(ctx, taskA))
	require.NoError(t, tasks.Create(ctx, taskB))

	rowCount := func(table string, id uuid.UUID) (n int) {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE id = $1", id).Scan(&n)
		}))
		return n
	}

	// Each case creates a row of tenant A under the given parents: first under
	// tenant B's (refused), then under tenant A's own (accepted, as the control).
	cases := []struct {
		name   string
		table  string
		create func(id, instanceID, taskID uuid.UUID) error
	}{
		{
			name: "a task on another tenant's instance", table: "workflow_task",
			create: func(id, instanceID, _ uuid.UUID) error {
				task := newTask(tenantA, instanceID)
				task.ID = id
				return tasks.Create(ctx, task)
			},
		},
		{
			name: "a task deferred from another tenant's task", table: "workflow_task",
			create: func(id, _, taskID uuid.UUID) error {
				task := newTask(tenantA, instA.ID)
				task.ID = id
				task.DeferredFromTaskID = &taskID
				return tasks.Create(ctx, task)
			},
		},
		{
			name: "an assignment on another tenant's task", table: "workflow_task_assignment",
			create: func(id, _, taskID uuid.UUID) error {
				assignment := newTaskAssignment(tenantA, taskID, uuid.New())
				assignment.ID = id
				return assignments.Create(ctx, assignment)
			},
		},
		{
			name: "an override on another tenant's instance", table: "assignee_overrides",
			create: func(id, instanceID, _ uuid.UUID) error {
				return overrides.Create(ctx, &domain.AssigneeOverride{
					ID: id, TenantID: tenantA, WorkflowInstanceID: instanceID, NodeKey: "finance/review",
					NewUserID: uuid.New(), ActorUserID: uuid.New(),
				})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refusedID, acceptedID := uuid.New(), uuid.New()

			err := tc.create(refusedID, instB.ID, taskB.ID)
			require.Error(t, err)
			assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
			assert.NotErrorIs(t, err, domain.ErrNotFound)
			assert.Zero(t, rowCount(tc.table, refusedID), "nothing was written under the other tenant's parent")

			require.NoError(t, tc.create(acceptedID, instA.ID, taskA.ID), "the same call under the tenant's own parent is accepted")
			assert.Equal(t, 1, rowCount(tc.table, acceptedID))
		})
	}
}
