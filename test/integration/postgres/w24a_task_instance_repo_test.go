//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestTaskRepo_GetByID_MapsEveryColumnOfTheStoredTask(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	tenantID := uuid.New()
	inst := w.seedInstance(t, tenantID)
	origin := newTask(tenantID, inst.ID)
	require.NoError(t, w.tasks.Create(ctx, origin))

	connector := "email"
	due := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Microsecond)
	followUp := due.Add(-24 * time.Hour)
	full := newTask(tenantID, inst.ID)
	full.NodeKey = "legal/sign"
	full.AssigneeMode = "all"
	full.ConnectorType = &connector
	full.ExtrasJSON = json.RawMessage(`{"channel":"ops"}`)
	full.DeferredFromTaskID = &origin.ID
	full.DueAt = &due
	full.FollowUpAt = &followUp
	full.VisitCount = 3
	require.NoError(t, w.tasks.Create(ctx, full))

	bare := newTask(tenantID, inst.ID)
	bare.NodeKey = "legal/bare"
	require.NoError(t, w.tasks.Create(ctx, bare))

	t.Run("every set column comes back", func(t *testing.T) {
		got, err := w.tasks.GetByID(ctx, tenantID, full.ID)

		require.NoError(t, err)
		assert.Equal(t, full.ID, got.ID)
		assert.Equal(t, tenantID, got.TenantID)
		assert.Equal(t, inst.ID, got.WorkflowInstanceID)
		assert.Equal(t, "legal/sign", got.NodeKey)
		assert.Equal(t, full.DepartmentID, got.DepartmentID)
		assert.Equal(t, domain.TaskStatusReady, got.Status)
		assert.Equal(t, "all", got.AssigneeMode)
		require.NotNil(t, got.ConnectorType)
		assert.Equal(t, "email", *got.ConnectorType)
		assert.JSONEq(t, `{"channel":"ops"}`, string(got.ExtrasJSON))
		require.NotNil(t, got.DeferredFromTaskID)
		assert.Equal(t, origin.ID, *got.DeferredFromTaskID)
		require.NotNil(t, got.DueAt)
		assert.True(t, due.Equal(*got.DueAt), "due_at %v", got.DueAt)
		require.NotNil(t, got.FollowUpAt)
		assert.True(t, followUp.Equal(*got.FollowUpAt), "follow_up_at %v", got.FollowUpAt)
		assert.EqualValues(t, 3, got.VisitCount)
		assert.EqualValues(t, 1, got.RecordVersion)
		assert.Nil(t, got.CompletedAt)
	})

	t.Run("columns left unset read as nil", func(t *testing.T) {
		got, err := w.tasks.GetByID(ctx, tenantID, bare.ID)

		require.NoError(t, err)
		assert.Nil(t, got.ConnectorType)
		assert.Nil(t, got.DeferredFromTaskID)
		assert.Nil(t, got.DueAt)
		assert.Nil(t, got.FollowUpAt)
		assert.Nil(t, got.CompletedAt)
	})

	t.Run("completed_at and the bumped version come back after a terminal status", func(t *testing.T) {
		updated, err := w.tasks.UpdateStatus(ctx, tenantID, full.ID, domain.TaskStatusCompleted, 1)
		require.NoError(t, err)

		got, err := w.tasks.GetByID(ctx, tenantID, full.ID)

		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusCompleted, got.Status)
		assert.EqualValues(t, 2, got.RecordVersion)
		require.NotNil(t, got.CompletedAt)
		require.NotNil(t, updated.CompletedAt)
		assert.True(t, updated.CompletedAt.Equal(*got.CompletedAt))
	})
}

func TestTaskRepo_GetByID_WhenTheTaskIsMissingOrTheQueryFails_ReturnsNoTask(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	tenantID := uuid.New()
	task := w.seedTask(t, tenantID)

	t.Run("an id nobody created is not found", func(t *testing.T) {
		got, err := w.tasks.GetByID(ctx, tenantID, uuid.New())

		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.Nil(t, got)
	})

	t.Run("a query that fails comes back unmapped with no task", func(t *testing.T) {
		var got *domain.Task

		err := w.withCancelledTx(tenantID, func(ctx context.Context) error {
			var getErr error
			got, getErr = w.tasks.GetByID(ctx, tenantID, task.ID)
			return getErr
		})

		require.ErrorIs(t, err, context.Canceled)
		w24aRequireNoDomainAnswer(t, err)
		assert.Nil(t, got)
	})
}

func TestTaskRepo_Create_AnIdThatAlreadyExists_ReturnsErrAlreadyExistsAndKeepsTheFirstTask(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	tenantID := uuid.New()
	first := w.seedTask(t, tenantID)

	dup := newTask(tenantID, first.WorkflowInstanceID)
	dup.ID = first.ID
	dup.NodeKey = "other/node"
	err := w.tasks.Create(ctx, dup)

	require.ErrorIs(t, err, domain.ErrAlreadyExists)
	got, getErr := w.tasks.GetByID(ctx, tenantID, first.ID)
	require.NoError(t, getErr)
	assert.Equal(t, first.NodeKey, got.NodeKey, "the first task is untouched")
}

func TestRepoExec_InsideATransaction_ReadsItsOwnUncommittedWritesWhileAPooledReadDoesNot(t *testing.T) {
	w := w24aNewWorld(t)
	tenantID := uuid.New()
	ctx := withGUC(context.Background(), tenantID)
	inst := newInstance(tenantID, time.Now().UTC())
	task := newTask(tenantID, inst.ID)

	err := w.transactor.RunInTx(ctx, func(txCtx context.Context) error {
		if err := w.instances.Create(txCtx, inst); err != nil {
			return err
		}
		if err := w.tasks.Create(txCtx, task); err != nil {
			return err
		}

		inTx, err := w.tasks.GetByID(txCtx, tenantID, task.ID)
		require.NoError(t, err, "a read in the transaction sees its own uncommitted insert")
		assert.Equal(t, task.ID, inTx.ID)

		pooled, err := w.tasks.GetByID(context.Background(), tenantID, task.ID)
		require.ErrorIs(t, err, domain.ErrNotFound, "a read on a pooled connection cannot see an uncommitted row")
		assert.Nil(t, pooled)
		return nil
	})
	require.NoError(t, err)

	after, err := w.tasks.GetByID(context.Background(), tenantID, task.ID)
	require.NoError(t, err, "the pooled read sees the row once the transaction committed")
	assert.Equal(t, task.ID, after.ID)
}

func TestRepoExec_ARolledBackTransaction_LeavesNothingForAPooledRead(t *testing.T) {
	w := w24aNewWorld(t)
	tenantID := uuid.New()
	ctx := withGUC(context.Background(), tenantID)
	inst := newInstance(tenantID, time.Now().UTC())
	rollback := assert.AnError

	err := w.transactor.RunInTx(ctx, func(txCtx context.Context) error {
		if err := w.instances.Create(txCtx, inst); err != nil {
			return err
		}
		return rollback
	})

	require.ErrorIs(t, err, rollback)
	_, getErr := w.instances.GetByID(context.Background(), tenantID, inst.ID)
	assert.ErrorIs(t, getErr, domain.ErrNotFound)
}

func TestTaskRepo_BumpRecordVersion_AnotherTenantsTaskAndConcurrentBumps(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()

	t.Run("another tenant's task is not found and keeps its version", func(t *testing.T) {
		owner, other := uuid.New(), uuid.New()
		task := w.seedTask(t, owner)

		got, err := w.tasks.BumpRecordVersion(ctx, other, task.ID, task.RecordVersion)

		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.Nil(t, got)
		stored, getErr := w.tasks.GetByID(ctx, owner, task.ID)
		require.NoError(t, getErr)
		assert.Equal(t, task.RecordVersion, stored.RecordVersion)
	})

	t.Run("a matching bump moves the version and updated_at forward", func(t *testing.T) {
		tenantID := uuid.New()
		task := w.seedTask(t, tenantID)

		got, err := w.tasks.BumpRecordVersion(ctx, tenantID, task.ID, task.RecordVersion)

		require.NoError(t, err)
		assert.Equal(t, task.RecordVersion+1, got.RecordVersion)
		assert.False(t, got.UpdatedAt.Before(task.UpdatedAt))
	})

	t.Run("two bumps of the same version at once: exactly one wins", func(t *testing.T) {
		tenantID := uuid.New()
		task := w.seedTask(t, tenantID)
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := w.tasks.BumpRecordVersion(ctx, tenantID, task.ID, task.RecordVersion)
				results <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var wins, conflicts int
		for err := range results {
			switch {
			case err == nil:
				wins++
			case assert.ErrorIs(t, err, domain.ErrRecordVersionConflict):
				conflicts++
			}
		}
		assert.Equal(t, 1, wins)
		assert.Equal(t, 1, conflicts)
		stored, err := w.tasks.GetByID(ctx, tenantID, task.ID)
		require.NoError(t, err)
		assert.Equal(t, task.RecordVersion+1, stored.RecordVersion, "the version moved once, not twice")
	})
}

func TestInstanceRepo_UpdateCurrentNodeKeys_AnotherTenantsInstance_IsNotFoundAndUnchanged(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	owner, other := uuid.New(), uuid.New()
	inst := w.seedInstance(t, owner)

	got, err := w.instances.UpdateCurrentNodeKeys(ctx, other, inst.ID, []string{"x/y"}, inst.RecordVersion)

	require.ErrorIs(t, err, domain.ErrNotFound)
	assert.Nil(t, got)
	stored, getErr := w.instances.GetByID(ctx, owner, inst.ID)
	require.NoError(t, getErr)
	assert.Equal(t, inst.CurrentNodeKeys, stored.CurrentNodeKeys)
	assert.Equal(t, inst.RecordVersion, stored.RecordVersion)
}

func TestInstanceRepo_GetLiveByBusinessKey_OnlyANonTerminalInstanceIsLive(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()

	cases := []struct {
		name     string
		status   domain.InstanceStatus
		wantLive bool
	}{
		{name: "running", status: domain.InstanceStatusRunning, wantLive: true},
		{name: "paused", status: domain.InstanceStatusPaused, wantLive: true},
		{name: "degraded", status: domain.InstanceStatusDegraded, wantLive: true},
		{name: "completed", status: domain.InstanceStatusCompleted, wantLive: false},
		{name: "terminated", status: domain.InstanceStatusTerminated, wantLive: false},
		{name: "failed", status: domain.InstanceStatusFailed, wantLive: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newInstance(tenantID, time.Now().UTC())
			inst.Status = tc.status
			require.NoError(t, w.instances.Create(ctx, inst))

			got, err := w.instances.GetLiveByBusinessKey(ctx, tenantID, inst.SubjectID, inst.BusinessKey)
			active, countErr := w.instances.CountActiveByWorkflow(ctx, tenantID, inst.WorkflowID)

			require.NoError(t, countErr)
			if !tc.wantLive {
				require.ErrorIs(t, err, domain.ErrNotFound)
				assert.Nil(t, got)
				assert.EqualValues(t, 0, active, "the active count agrees with the live lookup")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, inst.ID, got.ID)
			assert.Equal(t, tc.status, got.Status)
			assert.EqualValues(t, 1, active, "the active count agrees with the live lookup")
		})
	}
}

func TestInstanceRepo_RecordRunID_AnEmptyRunIDWritesNullAndKeepsTheVersion(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	tenantID := uuid.New()
	inst := w.seedInstance(t, tenantID)
	require.NoError(t, w.instances.RecordRunID(ctx, tenantID, inst.ID, "run-1"))

	require.NoError(t, w.instances.RecordRunID(ctx, tenantID, inst.ID, ""))

	var isNull bool
	require.NoError(t, w.superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT temporal_run_id IS NULL FROM workflow_instance WHERE id = $1`, inst.ID).Scan(&isNull)
	}))
	assert.True(t, isNull, "an empty run id clears the column to NULL, not an empty string")
	got, err := w.instances.GetByID(ctx, tenantID, inst.ID)
	require.NoError(t, err)
	assert.Equal(t, "", got.TemporalRunID)
	assert.Equal(t, inst.RecordVersion, got.RecordVersion)
}

func TestInstanceRepo_ListByTenant_FiltersCombineWithAndAndAnEmptyStatusSetIsUnfiltered(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	tenantID := uuid.New()
	now := time.Now().UTC()
	seed := func(status domain.InstanceStatus, age time.Duration) *domain.Instance {
		inst := newInstance(tenantID, now)
		inst.Status = status
		require.NoError(t, w.instances.Create(ctx, inst))
		seedInstanceAt(t, w.superPool, ctx, inst.ID, now.Add(-age))
		return inst
	}
	running := seed(domain.InstanceStatusRunning, 3*time.Minute)
	paused := seed(domain.InstanceStatusPaused, 2*time.Minute)
	completed := seed(domain.InstanceStatusCompleted, time.Minute)
	page := port.PageRequest{Limit: 10}

	statusOf := func(items []*domain.Instance) []uuid.UUID {
		ids := make([]uuid.UUID, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		return ids
	}
	ptr := func(s domain.InstanceStatus) *domain.InstanceStatus { return &s }

	cases := []struct {
		name   string
		filter port.InstanceListFilter
		want   []uuid.UUID
	}{
		{
			name:   "a status set returns each instance in it",
			filter: port.InstanceListFilter{Statuses: []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused}},
			want:   []uuid.UUID{paused.ID, running.ID},
		},
		{
			name: "a status and a status set must both match",
			filter: port.InstanceListFilter{
				Status:   ptr(domain.InstanceStatusRunning),
				Statuses: []domain.InstanceStatus{domain.InstanceStatusPaused},
			},
			want: []uuid.UUID{},
		},
		{
			name: "a status that is also in the set matches",
			filter: port.InstanceListFilter{
				Status:   ptr(domain.InstanceStatusPaused),
				Statuses: []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused},
			},
			want: []uuid.UUID{paused.ID},
		},
		{
			name:   "an empty status set filters nothing",
			filter: port.InstanceListFilter{Statuses: []domain.InstanceStatus{}},
			want:   []uuid.UUID{completed.ID, paused.ID, running.ID},
		},
		{
			name:   "an instance id returns that instance only",
			filter: port.InstanceListFilter{InstanceID: &paused.ID},
			want:   []uuid.UUID{paused.ID},
		},
		{
			name:   "a subject returns that subject's instances only",
			filter: port.InstanceListFilter{SubjectID: &running.SubjectID},
			want:   []uuid.UUID{running.ID},
		},
		{
			name: "an instance id of another subject returns nothing",
			filter: port.InstanceListFilter{
				InstanceID: &paused.ID,
				SubjectID:  &running.SubjectID,
			},
			want: []uuid.UUID{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items, next, err := w.instances.ListByTenant(ctx, tenantID, tc.filter, page)

			require.NoError(t, err)
			assert.Nil(t, next)
			assert.Equal(t, tc.want, statusOf(items), "newest first")
		})
	}

	t.Run("a status the database does not know fails and returns nothing", func(t *testing.T) {
		unknown := domain.InstanceStatus("NOT_A_STATUS")

		items, next, err := w.instances.ListByTenant(ctx, tenantID, port.InstanceListFilter{Status: &unknown}, page)

		require.Error(t, err)
		w24aRequireNoDomainAnswer(t, err)
		assert.Nil(t, items)
		assert.Nil(t, next)
	})

	t.Run("a page exactly as large as the result has no next cursor", func(t *testing.T) {
		items, next, err := w.instances.ListByTenant(ctx, tenantID, port.InstanceListFilter{}, port.PageRequest{Limit: 3})

		require.NoError(t, err)
		assert.Len(t, items, 3)
		assert.Nil(t, next, "no cursor when nothing is left")
	})

	t.Run("a page one short of the result has a cursor that leads to the last row", func(t *testing.T) {
		items, next, err := w.instances.ListByTenant(ctx, tenantID, port.InstanceListFilter{}, port.PageRequest{Limit: 2})
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.NotNil(t, next)
		assert.Equal(t, items[1].ID, next.ID)

		rest, last, err := w.instances.ListByTenant(ctx, tenantID, port.InstanceListFilter{}, port.PageRequest{Limit: 2, After: next})
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{running.ID}, statusOf(rest))
		assert.Nil(t, last)
	})
}

func TestReadScope_ListByTenant_ScopeWithoutDepartments_SeesOnlyItsLiveAssignments(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	tenantID := uuid.New()
	inst := w.seedInstance(t, tenantID)
	task := newTask(tenantID, inst.ID)
	require.NoError(t, w.tasks.Create(ctx, task))
	caller := uuid.New()
	assignment := newTaskAssignment(tenantID, task.ID, caller)
	require.NoError(t, w.assignments.Create(ctx, assignment))

	listBoth := func(t *testing.T, scope *port.ScopeFilter) (instances []*domain.Instance, tasks []*domain.Task) {
		t.Helper()
		var err error
		instances, _, err = w.instances.ListByTenant(ctx, tenantID, port.InstanceListFilter{Scope: scope}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		tasks, _, err = w.tasks.ListByTenant(ctx, tenantID, port.TaskListFilter{Scope: scope}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		return instances, tasks
	}

	t.Run("with a live assignment and no departments the caller sees the task and its instance", func(t *testing.T) {
		instances, tasks := listBoth(t, &port.ScopeFilter{DepartmentIDs: nil, CallerUserID: caller})

		require.Len(t, instances, 1)
		assert.Equal(t, inst.ID, instances[0].ID)
		require.Len(t, tasks, 1)
		assert.Equal(t, task.ID, tasks[0].ID)
	})

	t.Run("a different caller with no departments sees nothing", func(t *testing.T) {
		instances, tasks := listBoth(t, &port.ScopeFilter{DepartmentIDs: nil, CallerUserID: uuid.New()})

		assert.Empty(t, instances)
		assert.Empty(t, tasks)
	})

	t.Run("no scope at all is unscoped", func(t *testing.T) {
		instances, tasks := listBoth(t, nil)

		assert.Len(t, instances, 1)
		assert.Len(t, tasks, 1)
	})

	t.Run("after the assignment is vacated the caller sees nothing", func(t *testing.T) {
		_, err := w.assignments.Vacate(ctx, tenantID, assignment.ID)
		require.NoError(t, err)

		instances, tasks := listBoth(t, &port.ScopeFilter{DepartmentIDs: nil, CallerUserID: caller})

		assert.Empty(t, instances)
		assert.Empty(t, tasks)
	})
}
