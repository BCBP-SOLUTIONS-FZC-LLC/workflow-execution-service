package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestW21aListTasks_RequestDeadline_Returns503WithAFixedDetail(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"the bare deadline error", context.DeadlineExceeded},
		{"a deadline error wrapped by the service", fmt.Errorf("list tasks for the tenant: %w", context.DeadlineExceeded)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotTenant, gotCaller []uuid.UUID
			tasks := &fakeTaskService{list: func(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, _ port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
				gotTenant = append(gotTenant, tenantID)
				gotCaller = append(gotCaller, scope.CallerUserID)
				return port.PageResult[*port.Task]{}, tc.err
			}}
			router := newRouter(newHandler(tasks, &fakeEligibilityChecker{}))

			w := w21aServe(router, http.MethodGet, "/api/v1/tasks", "", true, nil)

			p := w21aDecodeProblem(t, w)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, w21aUpstreamType, p.Type)
			assert.Equal(t, "request timed out", p.Detail)
			assert.NotContains(t, w.Body.String(), "list tasks for the tenant", "the service's own error text stays out of the answer")
			assert.Equal(t, []uuid.UUID{testTenantID}, gotTenant)
			assert.Equal(t, []uuid.UUID{testUserID}, gotCaller)
		})
	}
}

func TestW21aListTasks_NoVisibleTasks_AnswersAnEmptyArrayAndNoCursor(t *testing.T) {
	rig := w21aNewRig()

	w := rig.get("/api/v1/tasks", true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	assert.Equal(t, []string{"items"}, w21aKeys(body), "no next_cursor key when there is no next page")
	assert.JSONEq(t, `[]`, string(body["items"]), "an empty list is [] and not null")
	require.Len(t, rig.tasks.lists, 1)
	assert.Equal(t, testTenantID, rig.tasks.lists[0].tenantID)
}

func TestW21aListTasks_ANextPage_AnswersTheCursorTheRepositoryGave(t *testing.T) {
	rig := w21aNewRig()
	task := &domain.Task{ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Review", Status: domain.TaskStatusReady, CreatedAt: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)}
	rig.tasks.rows = []*domain.Task{task}
	rig.tasks.next = &port.Cursor{CreatedAt: task.CreatedAt, ID: task.ID}

	w := rig.get("/api/v1/tasks", true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	pos, err := port.DecodeCursor(w21aString(t, body["next_cursor"]))
	require.NoError(t, err)
	assert.Equal(t, task.ID, pos.ID)
	assert.True(t, task.CreatedAt.Equal(pos.CreatedAt))
	assert.Len(t, w21aItems(t, body), 1)
}

func TestW21aListTasks_LimitQuery_ReachesTheRepositoryAtMostOneHundred(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantLimit int
	}{
		{"no limit", "", 25},
		{"one", "?limit=1", 1},
		{"exactly the maximum", "?limit=100", 100},
		{"one over the maximum", "?limit=101", 100},
		{"far over the maximum", "?limit=100000", 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w21aNewRig()

			w := rig.get("/api/v1/tasks"+tc.query, true, nil)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, rig.tasks.lists, 1)
			assert.Equal(t, testTenantID, rig.tasks.lists[0].tenantID)
			assert.Equal(t, tc.wantLimit, rig.tasks.lists[0].page.Limit)
		})
	}
}

func TestW21aListTasks_NonAdminCaller_IsScopedToTheirDepartmentsAndAssignments(t *testing.T) {
	dept := uuid.New()
	cases := []struct {
		name         string
		admin        bool
		headers      map[string]string
		wantScoped   bool
		wantDeptsLen int
	}{
		{name: "admin sees the tenant unscoped", admin: true},
		{name: "non-admin without an x-departments header is scoped to their own assignments", wantScoped: true},
		{name: "non-admin with only unusable entries is scoped to their own assignments", headers: map[string]string{"x-departments": "garbage, :x"}, wantScoped: true},
		{name: "non-admin with a department adds it to the scope", headers: map[string]string{"x-departments": dept.String() + ":approver"}, wantScoped: true, wantDeptsLen: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w21aNewRig()
			task := &domain.Task{ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Review", Status: domain.TaskStatusReady}
			rig.tasks.rows = []*domain.Task{task}
			rig.assignments.counts = map[uuid.UUID]int{task.ID: 1}

			w := rig.get("/api/v1/tasks", tc.admin, tc.headers)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			items := w21aItems(t, w21aObject(t, w.Body.Bytes()))
			require.Len(t, items, 1)
			assert.Equal(t, task.ID.String(), w21aString(t, items[0]["id"]))
			require.Len(t, rig.tasks.lists, 1)
			assert.Equal(t, testTenantID, rig.tasks.lists[0].tenantID)
			scope := rig.tasks.lists[0].filter.Scope
			if !tc.wantScoped {
				assert.Nil(t, scope)
				return
			}
			require.NotNil(t, scope, "a non-admin read always carries a scope, even with no departments")
			assert.Equal(t, testUserID, scope.CallerUserID)
			assert.Len(t, scope.DepartmentIDs, tc.wantDeptsLen)
			if tc.wantDeptsLen == 1 {
				assert.Equal(t, []uuid.UUID{dept}, scope.DepartmentIDs)
			}
		})
	}
}

func TestW21aGetTask_NoActiveAssignments_AnswersAnEmptyAssignmentsArray(t *testing.T) {
	rig := w21aNewRig()
	rig.tasks.rows = []*domain.Task{{ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/Review", Status: domain.TaskStatusReady, RecordVersion: 2}}

	w := rig.get("/api/v1/tasks/"+testTaskID.String(), true, nil)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w21aObject(t, w.Body.Bytes())
	assert.JSONEq(t, `[]`, string(body["assignments"]), "no assignments is [] and not null")
	assert.Equal(t, testTaskID.String(), w21aString(t, body["id"]))
	assert.Equal(t, []w21aLookup{{testTenantID, testTaskID}}, rig.assignments.lookups)
}

func TestW21aGetTask_NilUUID_IsLookedUpAndAnsweredNotFound(t *testing.T) {
	rig := w21aNewRig()
	rig.tasks.rows = []*domain.Task{{ID: testTaskID, TenantID: testTenantID}}

	w := rig.get("/api/v1/tasks/"+uuid.Nil.String(), true, nil)

	p := w21aDecodeProblem(t, w)
	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "TASK_NOT_FOUND", p.Code)
	assert.Equal(t, []w21aLookup{{testTenantID, uuid.Nil}}, rig.tasks.gets, "the nil id is parsed and looked up, not refused as malformed")
	assert.Empty(t, rig.assignments.lookups)
}

func TestW21aGetTask_DepartmentsHeaderWithABlankRole_StillMatchesByDepartment(t *testing.T) {
	taskDept := uuid.New()
	cases := []struct {
		name       string
		header     string
		wantStatus int
		wantCode   string
	}{
		{"spaces, a blank role and the matching entry last", uuid.NewString() + ":other, " + taskDept.String() + ":", http.StatusOK, ""},
		{"a blank role alone", taskDept.String() + ":", http.StatusOK, ""},
		{"a blank role on a department that is not the task's", uuid.NewString() + ":, " + uuid.NewString() + ":other", http.StatusForbidden, "NOT_AUTHORIZED_FOR_RESOURCE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w21aNewRig()
			rig.tasks.rows = []*domain.Task{{ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, DepartmentID: taskDept, NodeKey: "finance/Review", Status: domain.TaskStatusReady, RecordVersion: 2}}
			rig.assignments.active = []*domain.TaskAssignment{{ID: uuid.New(), TenantID: testTenantID, TaskID: testTaskID, UserID: uuid.New(), IsActive: true}}

			w := rig.get("/api/v1/tasks/"+testTaskID.String(), false, map[string]string{"x-departments": tc.header})

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			if tc.wantStatus == http.StatusOK {
				assert.Equal(t, testTaskID.String(), w21aString(t, w21aObject(t, w.Body.Bytes())["id"]))
			} else {
				assert.Equal(t, tc.wantCode, w21aDecodeProblem(t, w).Code)
			}
			assert.Equal(t, []w21aLookup{{testTenantID, testTaskID}}, rig.tasks.gets)
		})
	}
}
