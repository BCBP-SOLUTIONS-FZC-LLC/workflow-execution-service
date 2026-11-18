package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW16bTaskListDown = errors.New("w16b task list down")
	errW16bCountDown    = errors.New("w16b assignee count down")
)

type w16bTaskListCall struct {
	tenantID uuid.UUID
	filter   port.TaskListFilter
	limit    int
}

type w16bTaskListRepo struct {
	port.TaskRepository
	rows  []*domain.Task
	next  *port.Cursor
	err   error
	calls []w16bTaskListCall
}

func (r *w16bTaskListRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.TaskListFilter, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.calls = append(r.calls, w16bTaskListCall{tenantID, filter, page.Limit})
	return r.rows, r.next, r.err
}

type w16bCountCall struct {
	tenantID uuid.UUID
	taskIDs  []uuid.UUID
}

type w16bCountRepo struct {
	port.TaskAssignmentRepository
	counts map[uuid.UUID]int
	err    error
	calls  []w16bCountCall
}

func (r *w16bCountRepo) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	r.calls = append(r.calls, w16bCountCall{tenantID, append([]uuid.UUID(nil), taskIDs...)})
	return r.counts, r.err
}

func w16bListedTasks(tenantID uuid.UUID) []*domain.Task {
	instanceID := uuid.New()
	connector := "send-email"
	due := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	return []*domain.Task{
		{ID: uuid.MustParse("ffffffff-0000-0000-0000-000000000001"), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review",
			DepartmentID: uuid.New(), Status: domain.TaskStatusReady, RecordVersion: 3, AssigneeMode: "single", DueAt: &due,
			CreatedAt: time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC)},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/notify",
			DepartmentID: uuid.New(), Status: domain.TaskStatusInProgress, RecordVersion: 5, AssigneeMode: "all", ConnectorType: &connector,
			CreatedAt: time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)},
		{ID: uuid.MustParse("88888888-0000-0000-0000-000000000003"), TenantID: tenantID, WorkflowInstanceID: uuid.New(), NodeKey: "ops/audit",
			DepartmentID: uuid.New(), Status: domain.TaskStatusCompleted, RecordVersion: 9, AssigneeMode: "single",
			CreatedAt: time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)},
	}
}

func TestTaskService_List_AFailedAssigneeCountReturnsThatErrorAndNoRows(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	rows := w16bListedTasks(tenantID)
	tasks := &w16bTaskListRepo{rows: rows, next: &port.Cursor{CreatedAt: rows[2].CreatedAt, ID: rows[2].ID}}
	counts := &w16bCountRepo{err: errW16bCountDown}
	svc := &service.TaskService{Tasks: tasks, Assignments: counts}

	res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.TaskFilter{}, port.Page{Limit: 10})

	require.ErrorIs(t, err, errW16bCountDown)
	assert.NotErrorIs(t, err, port.ErrTaskNotFound, "the count error is not mapped to a missing task")
	assert.Empty(t, res.Items, "the rows that were read are discarded")
	assert.Empty(t, res.NextCursor)
	require.Len(t, tasks.calls, 1)
	assert.Equal(t, tenantID, tasks.calls[0].tenantID)
	assert.Equal(t, []w16bCountCall{{tenantID, []uuid.UUID{rows[0].ID, rows[1].ID, rows[2].ID}}}, counts.calls,
		"one count read, for the page's tasks in query order")
}

func TestTaskService_List_ARepositoryFailureIsReturnedWithoutCountingAssignees(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tasks := &w16bTaskListRepo{err: errW16bTaskListDown}
	counts := &w16bCountRepo{}
	svc := &service.TaskService{Tasks: tasks, Assignments: counts}

	res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.TaskFilter{}, port.Page{Limit: 10})

	require.ErrorIs(t, err, errW16bTaskListDown)
	assert.NotErrorIs(t, err, port.ErrTaskNotFound, "a failed read is not read as a missing task")
	assert.Empty(t, res.Items)
	require.Len(t, tasks.calls, 1)
	assert.Equal(t, tenantID, tasks.calls[0].tenantID)
	assert.Empty(t, counts.calls)
}

func TestTaskService_List_APageKeepsTheQueryOrderAndEachRowsOwnValues(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	rows := w16bListedTasks(tenantID)
	next := &port.Cursor{CreatedAt: rows[2].CreatedAt, ID: rows[2].ID}
	tasks := &w16bTaskListRepo{rows: rows, next: next}
	counts := &w16bCountRepo{counts: map[uuid.UUID]int{rows[0].ID: 1, rows[1].ID: 4}}
	svc := &service.TaskService{Tasks: tasks, Assignments: counts}

	res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.TaskFilter{}, port.Page{Limit: 3})

	require.NoError(t, err)
	require.Len(t, res.Items, 3)
	for i, want := range []struct {
		id        uuid.UUID
		nodeKey   string
		status    port.TaskStatus
		version   int64
		mode      string
		taskType  string
		assignees int
	}{
		{rows[0].ID, "finance/review", port.TaskStatusReady, 3, "single", "userTask", 1},
		{rows[1].ID, "finance/notify", port.TaskStatusInProgress, 5, "all", "serviceTask", 4},
		{rows[2].ID, "ops/audit", port.TaskStatusCompleted, 9, "single", "userTask", 0},
	} {
		got := res.Items[i]
		assert.Equal(t, want.id, got.ID, "row %d", i)
		assert.Equal(t, tenantID, got.TenantID, "row %d", i)
		assert.Equal(t, rows[i].WorkflowInstanceID, got.WorkflowInstanceID, "row %d", i)
		assert.Equal(t, rows[i].DepartmentID, got.DepartmentID, "row %d", i)
		assert.Equal(t, want.nodeKey, got.NodeKey, "row %d", i)
		assert.Equal(t, want.status, got.Status, "row %d", i)
		assert.Equal(t, want.version, got.RecordVersion, "row %d", i)
		assert.Equal(t, want.mode, got.AssigneeMode, "row %d", i)
		assert.Equal(t, want.taskType, got.TaskType, "row %d", i)
		assert.Equal(t, want.assignees, got.AssigneeCount, "row %d", i)
		assert.Equal(t, rows[i].CreatedAt, got.CreatedAt, "row %d", i)
	}
	assert.Equal(t, rows[0].DueAt, res.Items[0].DueAt)
	assert.Nil(t, res.Items[1].DueAt)
	cursor, err := port.DecodeCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, rows[2].ID, cursor.ID)
	assert.True(t, rows[2].CreatedAt.Equal(cursor.CreatedAt))
	assert.Equal(t, []w16bCountCall{{tenantID, []uuid.UUID{rows[0].ID, rows[1].ID, rows[2].ID}}}, counts.calls)
	require.Len(t, tasks.calls, 1)
	assert.Equal(t, 3, tasks.calls[0].limit)
}

func TestTaskService_List_AnAdminWithDepartmentsStillGetsNoVisibilityScope(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	for _, tc := range []struct {
		name    string
		scope   port.ReadScope
		wantNil bool
	}{
		{"an admin with department roles is not scoped", port.ReadScope{IsAdmin: true, CallerUserID: uuid.New(),
			Departments: []port.DepartmentRole{{DepartmentID: uuid.New(), Role: "member"}}}, true},
		{"a non-admin with no departments is scoped to their own assignments", port.ReadScope{CallerUserID: uuid.New()}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tasks := &w16bTaskListRepo{}
			svc := &service.TaskService{Tasks: tasks, Assignments: &w16bCountRepo{}}

			_, err := svc.List(context.Background(), tenantID, tc.scope, port.TaskFilter{}, port.Page{Limit: 5})

			require.NoError(t, err)
			require.Len(t, tasks.calls, 1)
			assert.Equal(t, tenantID, tasks.calls[0].tenantID)
			if tc.wantNil {
				assert.Nil(t, tasks.calls[0].filter.Scope)
				return
			}
			require.NotNil(t, tasks.calls[0].filter.Scope)
			assert.Equal(t, tc.scope.CallerUserID, tasks.calls[0].filter.Scope.CallerUserID)
			assert.Empty(t, tasks.calls[0].filter.Scope.DepartmentIDs)
		})
	}
}
