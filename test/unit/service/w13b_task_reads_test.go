package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// w13bActiveRepo records the tenant, user and page of each active-assignment page read.
type w13bActiveRepo struct {
	port.TaskAssignmentRepository
	err   error
	calls []w13bActiveCall
}

type w13bActiveCall struct {
	TenantID uuid.UUID
	UserID   uuid.UUID
	Limit    int
}

func (r *w13bActiveRepo) ListActiveByUserPaginated(_ context.Context, tenantID, userID uuid.UUID, page port.PageRequest) ([]port.ActiveUserTaskRow, *port.Cursor, error) {
	r.calls = append(r.calls, w13bActiveCall{TenantID: tenantID, UserID: userID, Limit: page.Limit})
	return nil, nil, r.err
}

// w13bVacateCaller ends the caller's assignment as a vacate does: it is no longer active and carries the time.
func w13bVacateCaller(w *cseTaskWorld, at time.Time) {
	w.assignments.rows[0].IsActive = false
	w.assignments.rows[0].VacatedAt = &at
}

func TestTaskGet_NonAdminCaller_NeedsAnActiveAssignmentOrADepartmentInScope(t *testing.T) {
	vacatedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name    string
		prepare func(w *cseTaskWorld, scope *port.ReadScope)
		allowed bool
		count   int
	}{
		{
			name:    "a vacated assignment no longer counts",
			prepare: func(w *cseTaskWorld, _ *port.ReadScope) { w13bVacateCaller(w, vacatedAt) },
		},
		{
			name: "a vacated assignment does not count while another user holds the task", prepare: func(w *cseTaskWorld, _ *port.ReadScope) {
				w13bVacateCaller(w, vacatedAt)
				w.assignments.rows = append(w.assignments.rows, &domain.TaskAssignment{ID: uuid.New(), TenantID: w.tenant, TaskID: w.task.ID, UserID: uuid.New(), IsActive: true})
			},
		},
		{
			name: "a vacated assignment is no help against a department outside the scope", prepare: func(w *cseTaskWorld, scope *port.ReadScope) {
				w13bVacateCaller(w, vacatedAt)
				scope.Departments = []port.DepartmentRole{{DepartmentID: uuid.New(), Role: "approver"}}
			},
		},
		{
			name: "a department in scope still allows the read after the assignment was vacated", prepare: func(w *cseTaskWorld, scope *port.ReadScope) {
				w13bVacateCaller(w, vacatedAt)
				w.task.DepartmentID = uuid.New()
				scope.Departments = []port.DepartmentRole{{DepartmentID: w.task.DepartmentID, Role: "approver"}}
			},
			allowed: true,
		},
		{name: "control: an active assignment allows the read", prepare: func(*cseTaskWorld, *port.ReadScope) {}, allowed: true, count: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := cseNewTaskWorld()
			scope := port.ReadScope{CallerUserID: w.user}
			tc.prepare(w, &scope)

			got, assignments, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, scope)

			assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.task.ID}}, w.tasks.getCalls)
			assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.task.ID}}, w.assignments.calls, "only the active assignments are asked for")
			if !tc.allowed {
				require.ErrorIs(t, err, port.ErrNotAuthorizedForRead, "the task exists, but the caller may not see it")
				assert.NotErrorIs(t, err, port.ErrTaskNotFound)
				assert.Nil(t, got)
				assert.Nil(t, assignments)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, w.task.ID, got.ID)
			assert.Equal(t, tc.count, got.AssigneeCount)
			assert.Len(t, assignments, tc.count)
		})
	}
}

func TestTaskGetAndActiveByUser_AssignmentListFails_ReturnTheRepositoryError(t *testing.T) {
	t.Run("get as an administrator", func(t *testing.T) {
		w := cseNewTaskWorld()
		w.assignments.err = errW13bRepo

		got, assignments, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, port.ReadScope{IsAdmin: true})

		require.Error(t, err)
		require.ErrorIs(t, err, errW13bRepo)
		assert.NotErrorIs(t, err, port.ErrTaskNotFound, "not mapped by the task error mapping")
		assert.Nil(t, got)
		assert.Nil(t, assignments)
		assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.task.ID}}, w.assignments.calls)
	})

	t.Run("get as an assignee", func(t *testing.T) {
		w := cseNewTaskWorld()
		w.assignments.err = errW13bRepo

		_, _, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, port.ReadScope{CallerUserID: w.user})

		require.ErrorIs(t, err, errW13bRepo)
		assert.NotErrorIs(t, err, port.ErrNotAuthorizedForRead, "a failed read is not a refusal")
	})

	t.Run("active tasks of a user", func(t *testing.T) {
		repo := &w13bActiveRepo{err: errW13bRepo}
		svc := &service.TaskService{Assignments: repo}
		tenantID, userID := uuid.New(), uuid.New()

		got, err := svc.ActiveByUser(context.Background(), tenantID, userID, port.Page{Limit: 7})

		require.ErrorIs(t, err, errW13bRepo)
		assert.Empty(t, got.Items)
		assert.Empty(t, got.NextCursor)
		assert.Equal(t, []w13bActiveCall{{TenantID: tenantID, UserID: userID, Limit: 7}}, repo.calls)
	})

	t.Run("control: the same reads succeed when the repository does", func(t *testing.T) {
		w := cseNewTaskWorld()

		got, assignments, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, port.ReadScope{IsAdmin: true})

		require.NoError(t, err)
		assert.Equal(t, w.task.ID, got.ID)
		assert.Len(t, assignments, 1)
	})

	t.Run("control: active tasks of a user when the repository succeeds", func(t *testing.T) {
		repo := &w13bActiveRepo{}
		svc := &service.TaskService{Assignments: repo}
		tenantID, userID := uuid.New(), uuid.New()

		got, err := svc.ActiveByUser(context.Background(), tenantID, userID, port.Page{Limit: 7})

		require.NoError(t, err)
		assert.Empty(t, got.Items)
		assert.Empty(t, got.NextCursor)
		assert.Equal(t, []w13bActiveCall{{TenantID: tenantID, UserID: userID, Limit: 7}}, repo.calls)
	})
}
