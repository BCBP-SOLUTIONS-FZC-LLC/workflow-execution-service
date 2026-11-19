//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w24aWorld struct {
	superPool   *pgcommon.Pool
	appPool     *pgcommon.Pool
	instances   *postgres.InstanceRepo
	tasks       *postgres.TaskRepo
	assignments *postgres.TaskAssignmentRepo
	transactor  *postgres.Transactor
}

func w24aNewWorld(t *testing.T) *w24aWorld {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	return &w24aWorld{
		superPool:   superPool,
		appPool:     appPool,
		instances:   postgres.NewInstanceRepo(appPool),
		tasks:       postgres.NewTaskRepo(appPool),
		assignments: postgres.NewTaskAssignmentRepo(appPool),
		transactor:  postgres.NewTransactor(appPool),
	}
}

func (w *w24aWorld) seedInstance(t *testing.T, tenantID uuid.UUID) *domain.Instance {
	t.Helper()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, w.instances.Create(context.Background(), inst))
	return inst
}

func (w *w24aWorld) seedTask(t *testing.T, tenantID uuid.UUID) *domain.Task {
	t.Helper()
	inst := w.seedInstance(t, tenantID)
	task := newTask(tenantID, inst.ID)
	require.NoError(t, w.tasks.Create(context.Background(), task))
	return task
}

func (w *w24aWorld) seedAssignment(t *testing.T, tenantID uuid.UUID) (*domain.Task, *domain.TaskAssignment) {
	t.Helper()
	task := w.seedTask(t, tenantID)
	a := newTaskAssignment(tenantID, task.ID, uuid.New())
	require.NoError(t, w.assignments.Create(context.Background(), a))
	return task, a
}

// w24aWithCancelledTx runs fn on a context that is already cancelled but still
// carries the open transaction, so the failure comes from the query itself.
func (w *w24aWorld) withCancelledTx(tenantID uuid.UUID, fn func(ctx context.Context) error) error {
	return w.transactor.RunInTx(withGUC(context.Background(), tenantID), func(txCtx context.Context) error {
		cancelled, cancel := context.WithCancel(txCtx)
		cancel()
		return fn(cancelled)
	})
}

func w24aRequireNoDomainAnswer(t *testing.T, err error) {
	t.Helper()
	for _, answer := range []error{
		domain.ErrNotFound, domain.ErrRecordVersionConflict, domain.ErrAlreadyExists,
		domain.ErrDuplicateActiveAssignment, domain.ErrDuplicateBusinessKey,
	} {
		assert.NotErrorIs(t, err, answer, "an unmapped database error must not look like a client error")
	}
}

func TestTaskAssignmentRepo_Create_DuplicateKeys_MapToTheirDomainErrors(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		second  func(first *domain.TaskAssignment) *domain.TaskAssignment
		wantErr error
	}{
		{
			name: "same assignment id on the same task",
			second: func(first *domain.TaskAssignment) *domain.TaskAssignment {
				dup := newTaskAssignment(first.TenantID, first.TaskID, uuid.New())
				dup.ID = first.ID
				return dup
			},
			wantErr: domain.ErrAlreadyExists,
		},
		{
			name: "second active assignment of the same user on the same task",
			second: func(first *domain.TaskAssignment) *domain.TaskAssignment {
				return newTaskAssignment(first.TenantID, first.TaskID, first.UserID)
			},
			wantErr: domain.ErrDuplicateActiveAssignment,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			task, first := w.seedAssignment(t, tenantID)

			err := w.assignments.Create(ctx, tc.second(first))

			require.ErrorIs(t, err, tc.wantErr)
			active, listErr := w.assignments.ListActiveByTask(ctx, tenantID, task.ID)
			require.NoError(t, listErr)
			require.Len(t, active, 1, "the refused insert left no second row")
			assert.Equal(t, first.ID, active[0].ID)
			assert.Equal(t, first.UserID, active[0].UserID)
		})
	}
}

func TestTaskAssignmentRepo_ListActive_ACallerOfAnotherTenant_SeesNoAssignments(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()
	owner, other := uuid.New(), uuid.New()
	task, assignment := w.seedAssignment(t, owner)

	t.Run("the owning tenant sees the assignment", func(t *testing.T) {
		byTask, err := w.assignments.ListActiveByTask(ctx, owner, task.ID)
		require.NoError(t, err)
		require.Len(t, byTask, 1)
		assert.Equal(t, assignment.ID, byTask[0].ID)
		assert.Equal(t, owner, byTask[0].TenantID)
	})

	t.Run("another tenant asking for that task gets an empty list", func(t *testing.T) {
		byTask, err := w.assignments.ListActiveByTask(ctx, other, task.ID)
		require.NoError(t, err)
		assert.Empty(t, byTask)
	})

	t.Run("another tenant asking for that user gets an empty list", func(t *testing.T) {
		byUser, err := w.assignments.ListActiveByUser(ctx, other, assignment.UserID)
		require.NoError(t, err)
		assert.Empty(t, byUser)
	})
}

func TestTaskAssignmentRepo_ListActive_WhenTheQueryFails_ReturnsTheErrorUnmappedAndNoRows(t *testing.T) {
	w := w24aNewWorld(t)
	tenantID := uuid.New()
	task, assignment := w.seedAssignment(t, tenantID)

	cases := []struct {
		name string
		call func(ctx context.Context) (int, bool, error)
	}{
		{
			name: "by task",
			call: func(ctx context.Context) (int, bool, error) {
				got, err := w.assignments.ListActiveByTask(ctx, tenantID, task.ID)
				return len(got), got == nil, err
			},
		},
		{
			name: "by user",
			call: func(ctx context.Context) (int, bool, error) {
				got, err := w.assignments.ListActiveByUser(ctx, tenantID, assignment.UserID)
				return len(got), got == nil, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotLen int
			var gotNil bool

			err := w.withCancelledTx(tenantID, func(ctx context.Context) error {
				var callErr error
				gotLen, gotNil, callErr = tc.call(ctx)
				return callErr
			})

			require.ErrorIs(t, err, context.Canceled)
			w24aRequireNoDomainAnswer(t, err)
			assert.Zero(t, gotLen)
			assert.True(t, gotNil, "a failed list returns no slice at all")
		})
	}
}

func TestTaskAssignmentRepo_Complete_ReplayedAfterItCommitted_ConflictsAndChangesNothing(t *testing.T) {
	w := w24aNewWorld(t)
	tenantID := uuid.New()
	task, assignment := w.seedAssignment(t, tenantID)
	ctx := withGUC(context.Background(), tenantID)
	staleVersion := task.RecordVersion

	first := json.RawMessage(`{"attempt":"first"}`)
	require.NoError(t, w.transactor.RunInTx(ctx, func(txCtx context.Context) error {
		_, err := w.assignments.Complete(txCtx, tenantID, assignment.ID, first, staleVersion)
		return err
	}))

	err := w.transactor.RunInTx(ctx, func(txCtx context.Context) error {
		_, err := w.assignments.Complete(txCtx, tenantID, assignment.ID, json.RawMessage(`{"attempt":"replay"}`), staleVersion)
		return err
	})

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	got, getErr := w.assignments.GetByID(context.Background(), tenantID, assignment.ID)
	require.NoError(t, getErr)
	assert.JSONEq(t, string(first), string(got.ResultJSON), "the replay's rolled-back write did not replace the first result")
	assert.False(t, got.IsActive)
	require.NotNil(t, got.CompletedAt)
	gotTask, taskErr := w.tasks.GetByID(context.Background(), tenantID, task.ID)
	require.NoError(t, taskErr)
	assert.Equal(t, staleVersion+1, gotTask.RecordVersion, "only the first Complete bumped the task version")
}

func TestRepos_ForeignKeyAndNotNullViolations_ReachTheCallerUnmapped(t *testing.T) {
	w := w24aNewWorld(t)
	ctx := context.Background()

	t.Run("a task for an instance that does not exist", func(t *testing.T) {
		tenantID := uuid.New()
		task := newTask(tenantID, uuid.New())

		err := w.tasks.Create(ctx, task)

		require.Error(t, err)
		w24aRequireNoDomainAnswer(t, err)
		_, getErr := w.tasks.GetByID(ctx, tenantID, task.ID)
		assert.ErrorIs(t, getErr, domain.ErrNotFound, "nothing was stored")
	})

	t.Run("an assignment for a task that does not exist", func(t *testing.T) {
		tenantID := uuid.New()
		a := newTaskAssignment(tenantID, uuid.New(), uuid.New())

		err := w.assignments.Create(ctx, a)

		require.Error(t, err)
		w24aRequireNoDomainAnswer(t, err)
		_, getErr := w.assignments.GetByID(ctx, tenantID, a.ID)
		assert.ErrorIs(t, getErr, domain.ErrNotFound, "nothing was stored")
	})

	t.Run("a task with a negative visit count breaks a check constraint", func(t *testing.T) {
		tenantID := uuid.New()
		inst := w.seedInstance(t, tenantID)
		task := newTask(tenantID, inst.ID)
		task.VisitCount = -1

		err := w.tasks.Create(ctx, task)

		require.Error(t, err)
		w24aRequireNoDomainAnswer(t, err)
		_, getErr := w.tasks.GetByID(ctx, tenantID, task.ID)
		assert.ErrorIs(t, getErr, domain.ErrNotFound, "nothing was stored")
	})

	t.Run("replacing the current node keys with a nil list", func(t *testing.T) {
		tenantID := uuid.New()
		inst := w.seedInstance(t, tenantID)

		got, err := w.instances.UpdateCurrentNodeKeys(ctx, tenantID, inst.ID, nil, inst.RecordVersion)

		require.Error(t, err)
		w24aRequireNoDomainAnswer(t, err)
		assert.Nil(t, got)
		stored, getErr := w.instances.GetByID(ctx, tenantID, inst.ID)
		require.NoError(t, getErr)
		assert.Equal(t, inst.CurrentNodeKeys, stored.CurrentNodeKeys)
		assert.Equal(t, inst.RecordVersion, stored.RecordVersion)
	})
}
