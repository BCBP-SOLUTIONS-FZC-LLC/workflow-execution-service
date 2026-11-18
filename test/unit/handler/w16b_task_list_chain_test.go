package handler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW16bTaskReadDown = errors.New("w16b task read down: password=hunter2")
	errW16bCountReadErr = errors.New("w16b count read down: password=hunter2")
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

func w16bTaskListRouter(tasks *w16bTaskListRepo, counts *w16bCountRepo) *gin.Engine {
	svc := &service.TaskService{Tasks: tasks, Assignments: counts}
	return newRouter(handler.New(handler.Services{Tasks: svc, Eligibility: &fakeEligibilityChecker{}}))
}

type w16bTaskListProblem struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func TestListTasks_WhenTheRepositoryQueryFails_Returns500InternalErrorWithoutTheCause(t *testing.T) {
	tasks := &w16bTaskListRepo{err: errW16bTaskReadDown}
	counts := &w16bCountRepo{}
	router := w16bTaskListRouter(tasks, counts)

	w := do(router, req(http.MethodGet, "/api/v1/tasks", nil))

	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	var p w16bTaskListProblem
	decodeJSON(t, w.Body, &p)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, "an unexpected error occurred", p.Detail)
	assert.NotContains(t, w.Body.String(), "hunter2", "the failure text is not echoed")
	require.Len(t, tasks.calls, 1)
	assert.Equal(t, testTenantID, tasks.calls[0].tenantID)
	assert.Empty(t, counts.calls, "no assignee count is read for a failed page")
}

func TestListTasks_WhenTheAssigneeCountFails_Returns500InternalErrorAndNoItems(t *testing.T) {
	rows := []*domain.Task{
		{ID: uuid.New(), TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/review", Status: domain.TaskStatusReady, RecordVersion: 1},
		{ID: uuid.New(), TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/notify", Status: domain.TaskStatusReady, RecordVersion: 1},
	}
	tasks := &w16bTaskListRepo{rows: rows, next: &port.Cursor{CreatedAt: time.Unix(5, 0), ID: rows[1].ID}}
	counts := &w16bCountRepo{err: errW16bCountReadErr}
	router := w16bTaskListRouter(tasks, counts)

	w := do(router, req(http.MethodGet, "/api/v1/tasks", nil))

	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	var p w16bTaskListProblem
	decodeJSON(t, w.Body, &p)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, "an unexpected error occurred", p.Detail)
	assert.NotContains(t, w.Body.String(), "hunter2")
	assert.NotContains(t, w.Body.String(), "items", "no page is returned for a failed count")
	assert.NotContains(t, w.Body.String(), "next_cursor")
	assert.Equal(t, []w16bCountCall{{testTenantID, []uuid.UUID{rows[0].ID, rows[1].ID}}}, counts.calls)
}

func TestListTasks_SeveralTasksAreReturnedOneToOneInRepositoryOrder(t *testing.T) {
	instanceID := uuid.New()
	connector := "send-email"
	due := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	rows := []*domain.Task{
		{ID: uuid.MustParse("ffffffff-0000-0000-0000-000000000001"), TenantID: testTenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review",
			DepartmentID: uuid.New(), Status: domain.TaskStatusReady, RecordVersion: 3, AssigneeMode: "single", DueAt: &due,
			CreatedAt: time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC)},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), TenantID: testTenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/notify",
			DepartmentID: uuid.New(), Status: domain.TaskStatusInProgress, RecordVersion: 5, AssigneeMode: "all", ConnectorType: &connector,
			CreatedAt: time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)},
		{ID: uuid.MustParse("88888888-0000-0000-0000-000000000003"), TenantID: testTenantID, WorkflowInstanceID: uuid.New(), NodeKey: "ops/audit",
			DepartmentID: uuid.New(), Status: domain.TaskStatusCompleted, RecordVersion: 9, AssigneeMode: "single",
			CreatedAt: time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)},
	}
	tasks := &w16bTaskListRepo{rows: rows, next: &port.Cursor{CreatedAt: rows[2].CreatedAt, ID: rows[2].ID}}
	counts := &w16bCountRepo{counts: map[uuid.UUID]int{rows[0].ID: 1, rows[1].ID: 4}}
	router := w16bTaskListRouter(tasks, counts)

	r := req(http.MethodGet, "/api/v1/tasks?limit=3", nil)
	r.Header.Set("x-tenant-roles", "tenant_admin")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var body struct {
		Items []struct {
			ID                 uuid.UUID  `json:"id"`
			TenantID           uuid.UUID  `json:"tenant_id"`
			WorkflowInstanceID uuid.UUID  `json:"workflow_instance_id"`
			DepartmentID       uuid.UUID  `json:"department_id"`
			NodeKey            string     `json:"node_key"`
			TaskType           string     `json:"task_type"`
			Status             string     `json:"status"`
			RecordVersion      int64      `json:"record_version"`
			AssigneeMode       string     `json:"assignee_mode"`
			AssigneeCount      *int       `json:"assignee_count"`
			DueAt              *time.Time `json:"due_at"`
		} `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &body)
	require.Len(t, body.Items, 3)
	wantType := []string{"userTask", "serviceTask", "userTask"}
	wantStatus := []string{"READY", "IN_PROGRESS", "COMPLETED"}
	wantCount := []int{1, 4, 0}
	for i, got := range body.Items {
		label := fmt.Sprintf("item %d", i)
		assert.Equal(t, rows[i].ID, got.ID, label)
		assert.Equal(t, testTenantID, got.TenantID, label)
		assert.Equal(t, rows[i].WorkflowInstanceID, got.WorkflowInstanceID, label)
		assert.Equal(t, rows[i].DepartmentID, got.DepartmentID, label)
		assert.Equal(t, rows[i].NodeKey, got.NodeKey, label)
		assert.Equal(t, wantType[i], got.TaskType, label)
		assert.Equal(t, wantStatus[i], got.Status, label)
		assert.Equal(t, rows[i].RecordVersion, got.RecordVersion, label)
		assert.Equal(t, rows[i].AssigneeMode, got.AssigneeMode, label)
		require.NotNil(t, got.AssigneeCount, label)
		assert.Equal(t, wantCount[i], *got.AssigneeCount, label)
	}
	require.NotNil(t, body.Items[0].DueAt)
	assert.True(t, due.Equal(*body.Items[0].DueAt))
	assert.Nil(t, body.Items[1].DueAt)
	cursor, err := port.DecodeCursor(body.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, rows[2].ID, cursor.ID)
	assert.True(t, rows[2].CreatedAt.Equal(cursor.CreatedAt))
	assert.Equal(t, []w16bCountCall{{testTenantID, []uuid.UUID{rows[0].ID, rows[1].ID, rows[2].ID}}}, counts.calls)
	require.Len(t, tasks.calls, 1)
	assert.Equal(t, 3, tasks.calls[0].limit)
}

func TestListTasks_OnlyAnAdminRoleLiftsTheVisibilityScope(t *testing.T) {
	dept := uuid.New()
	cases := []struct {
		name    string
		roles   string
		wantNil bool
	}{
		{"tenant_admin", "tenant_admin", true},
		{"tenant_owner", "tenant_owner", true},
		{"an admin role among others", "tenant_member,tenant_owner", true},
		{"a role that is not an admin role", "tenant_member", false},
		{"no roles at all", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &w16bTaskListRepo{}
			router := w16bTaskListRouter(tasks, &w16bCountRepo{})

			r := req(http.MethodGet, "/api/v1/tasks", nil)
			r.Header.Set("x-departments", dept.String()+":approver")
			if tc.roles != "" {
				r.Header.Set("x-tenant-roles", tc.roles)
			}
			w := do(router, r)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, tasks.calls, 1)
			assert.Equal(t, testTenantID, tasks.calls[0].tenantID)
			if tc.wantNil {
				assert.Nil(t, tasks.calls[0].filter.Scope, "an admin's list is not narrowed, even with departments in the header")
				return
			}
			require.NotNil(t, tasks.calls[0].filter.Scope)
			assert.Equal(t, testUserID, tasks.calls[0].filter.Scope.CallerUserID)
			assert.Equal(t, []uuid.UUID{dept}, tasks.calls[0].filter.Scope.DepartmentIDs)
		})
	}
}
