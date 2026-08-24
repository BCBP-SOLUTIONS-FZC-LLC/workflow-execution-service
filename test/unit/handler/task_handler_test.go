package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func newTestTask() *port.Task {
	return &port.Task{
		ID:                 testTaskID,
		WorkflowInstanceID: testInstID,
		NodeKey:            "review_finance",
		DepartmentID:       uuid.New(),
		Status:             port.TaskStatusReady,
		RecordVersion:      4,
		AssigneeMode:       "single",
		CreatedAt:          time.Now(),
	}
}

// newFullyPopulatedTask returns a *port.Task with every field the handler's
// response mapper knows how to surface set to a realistic non-zero value —
// used to assert toTaskResp doesn't silently drop any of them.
func newFullyPopulatedTask() *port.Task {
	dueAt := time.Now().Add(24 * time.Hour)
	deferredFrom := uuid.New()
	return &port.Task{
		ID:                 testTaskID,
		TenantID:           testTenantID,
		WorkflowInstanceID: testInstID,
		NodeKey:            "review_finance",
		TaskType:           "userTask",
		DepartmentID:       uuid.New(),
		Status:             port.TaskStatusReady,
		RecordVersion:      4,
		AssigneeMode:       "single",
		AssigneeCount:      1,
		DueAt:              &dueAt,
		DeferredFromTaskID: &deferredFrom,
		CreatedAt:          time.Now(),
	}
}

// newFullyPopulatedAssignment returns a *port.TaskAssignment with every field
// the handler's response mapper knows how to surface set to a realistic
// non-zero value — used to assert toTaskAssignmentResp doesn't silently drop
// any of them.
func newFullyPopulatedAssignment() *port.TaskAssignment {
	assignedBy := uuid.New()
	assignedAt := time.Now().Add(-2 * time.Hour)
	claimedAt := time.Now().Add(-1 * time.Hour)
	completedAt := time.Now()
	return &port.TaskAssignment{
		ID:          uuid.New(),
		UserID:      testUserID,
		AssignedBy:  &assignedBy,
		Reason:      "eligibility override",
		IsLead:      true,
		IsActive:    true,
		AssignedAt:  &assignedAt,
		ClaimedAt:   &claimedAt,
		CompletedAt: &completedAt,
		ResultJSON:  json.RawMessage(`{"decision":"approve"}`),
	}
}

func TestListTasks(t *testing.T) {
	var gotScope port.ReadScope
	fake := &fakeTaskService{
		list: func(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
			assert.Equal(t, testTenantID, tenantID)
			gotScope = scope
			return port.PageResult[*port.Task]{Items: []*port.Task{newTestTask()}, NextCursor: "next"}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	deptA, deptB := uuid.New(), uuid.New()
	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-departments", fmt.Sprintf("%s:approver, %s:reviewer", deptA, deptB))
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testUserID, gotScope.CallerUserID)
	assert.Equal(t, []port.DepartmentRole{
		{DepartmentID: deptA, Role: "approver"},
		{DepartmentID: deptB, Role: "reviewer"},
	}, gotScope.Departments)
	assert.False(t, gotScope.IsAdmin)

	var body struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Len(t, body.Items, 1)
	assert.Equal(t, "next", body.NextCursor)
}

// TestListTasks_ParsesAssigneeAndDueBeforeFilters asserts the assignee_user_id
// and due_before query parameters decode into port.TaskFilter correctly.
func TestListTasks_ParsesAssigneeAndDueBeforeFilters(t *testing.T) {
	assigneeID := uuid.New()
	var gotFilter port.TaskFilter
	fake := &fakeTaskService{
		list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, filter port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
			gotFilter = filter
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	url := "/api/v1/tasks?assignee_user_id=" + assigneeID.String() + "&due_before=2026-08-01T00:00:00Z"
	w := do(router, req(http.MethodGet, url, nil))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotFilter.AssigneeUserID)
	assert.Equal(t, assigneeID, *gotFilter.AssigneeUserID)
	require.NotNil(t, gotFilter.DueBefore)
	assert.Equal(t, "2026-08-01T00:00:00Z", gotFilter.DueBefore.Format(time.RFC3339))
}

// TestListTasks_InvalidAssigneeUserID asserts a malformed assignee_user_id
// query parameter is rejected with 400, not silently ignored.
func TestListTasks_InvalidAssigneeUserID(t *testing.T) {
	fake := &fakeTaskService{}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?assignee_user_id=not-a-uuid", nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestListTasks_InvalidDueBefore asserts a malformed due_before query
// parameter is rejected with 400, not silently ignored.
func TestListTasks_InvalidDueBefore(t *testing.T) {
	fake := &fakeTaskService{}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?due_before=not-a-date", nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestGetTask_PopulatesPayloadJSON asserts the detail endpoint surfaces the
// task's ExtrasJSON as payload_json.
func TestGetTask_PopulatesPayloadJSON(t *testing.T) {
	task := newTestTask()
	task.ExtrasJSON = json.RawMessage(`{"foo":"bar"}`)
	fake := &fakeTaskService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			return task, nil, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, map[string]any{"foo": "bar"}, body["payload_json"])
}

// TestListTasks_PopulatesWidenedFields asserts toTaskResp's tenant_id,
// due_at and deferred_from_task_id fields — previously dropped by an
// unfinished mapper — now appear in the list endpoint's JSON response.
func TestListTasks_PopulatesWidenedFields(t *testing.T) {
	task := newFullyPopulatedTask()
	fake := &fakeTaskService{
		list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
			return port.PageResult[*port.Task]{Items: []*port.Task{task}}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks", nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []map[string]any `json:"items"`
	}
	decodeJSON(t, w.Body, &body)
	require.Len(t, body.Items, 1)
	item := body.Items[0]
	assert.Equal(t, testTenantID.String(), item["tenant_id"])
	assert.NotEmpty(t, item["due_at"])
	assert.Equal(t, task.DeferredFromTaskID.String(), item["deferred_from_task_id"])
}

func TestListTasks_AdminRole(t *testing.T) {
	var gotScope port.ReadScope
	fake := &fakeTaskService{
		list: func(_ context.Context, _ uuid.UUID, scope port.ReadScope, _ port.TaskFilter, _ port.Page) (port.PageResult[*port.Task], error) {
			gotScope = scope
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-tenant-roles", "tenant_admin")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, gotScope.IsAdmin)
}

func TestGetTask(t *testing.T) {
	fake := &fakeTaskService{
		get: func(_ context.Context, tenantID, taskID uuid.UUID, _ port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			assert.Equal(t, testTenantID, tenantID)
			assert.Equal(t, testTaskID, taskID)
			return newTestTask(), []*port.TaskAssignment{{ID: uuid.New(), UserID: testUserID, IsLead: true, IsActive: true}}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		ID          uuid.UUID        `json:"id"`
		Assignments []map[string]any `json:"assignments"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, testTaskID, body.ID)
	assert.Len(t, body.Assignments, 1)
}

// TestGetTask_PopulatesWidenedFields asserts the detail endpoint's task and
// assignment JSON carries every field toTaskResp/toTaskAssignmentResp were
// previously dropping: tenant_id/due_at/deferred_from_task_id on the task,
// and assigned_by/reason/assigned_at/claimed_at/completed_at/result_json/
// is_lead on each assignment.
func TestGetTask_PopulatesWidenedFields(t *testing.T) {
	task := newFullyPopulatedTask()
	assignment := newFullyPopulatedAssignment()
	fake := &fakeTaskService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			return task, []*port.TaskAssignment{assignment}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	decodeJSON(t, w.Body, &body)

	assert.Equal(t, testTenantID.String(), body["tenant_id"])
	assert.NotEmpty(t, body["due_at"])
	assert.Equal(t, task.DeferredFromTaskID.String(), body["deferred_from_task_id"])

	assignments, ok := body["assignments"].([]any)
	require.True(t, ok)
	require.Len(t, assignments, 1)
	a, ok := assignments[0].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, assignment.AssignedBy.String(), a["assigned_by"])
	assert.Equal(t, assignment.Reason, a["reason"])
	assert.Equal(t, true, a["is_lead"])
	assert.NotEmpty(t, a["assigned_at"])
	assert.NotEmpty(t, a["claimed_at"])
	assert.NotEmpty(t, a["completed_at"])
	assert.Equal(t, map[string]any{"decision": "approve"}, a["result_json"])
}

func TestGetTask_NotAuthorized(t *testing.T) {
	fake := &fakeTaskService{
		get: func(_ context.Context, _, _ uuid.UUID, _ port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			return nil, nil, port.ErrNotAuthorizedForRead
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	require.Equal(t, http.StatusForbidden, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "NOT_AUTHORIZED_FOR_RESOURCE", body.Code)
}

func TestGetTask_NotFound(t *testing.T) {
	fake := &fakeTaskService{
		get: func(_ context.Context, _, _ uuid.UUID, _ port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			return nil, nil, port.ErrTaskNotFound
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestClaimTask(t *testing.T) {
	fake := &fakeTaskService{
		claim: func(_ context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
			assert.Equal(t, testTenantID, tenantID)
			assert.Equal(t, testTaskID, taskID)
			assert.Equal(t, testUserID, userID)
			assert.Equal(t, int64(4), recordVersion)
			return newTestTask(), nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 4}))

	assert.Equal(t, http.StatusAccepted, w.Code)
}

// TestClaimTask_PopulatesWidenedFields asserts a mutation endpoint's
// toTaskResp-shaped response (shared by Claim/Complete/Defer/Reassign) also
// carries the widened tenant_id/due_at/deferred_from_task_id fields, not just
// the read endpoints.
func TestClaimTask_PopulatesWidenedFields(t *testing.T) {
	task := newFullyPopulatedTask()
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return task, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 4}))

	require.Equal(t, http.StatusAccepted, w.Code)
	var body map[string]any
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, testTenantID.String(), body["tenant_id"])
	assert.NotEmpty(t, body["due_at"])
	assert.Equal(t, task.DeferredFromTaskID.String(), body["deferred_from_task_id"])
}

func TestClaimTask_RecordVersionConflict(t *testing.T) {
	fake := &fakeTaskService{
		claim: func(_ context.Context, _, _, _ uuid.UUID, _ int64) (*port.Task, error) {
			return nil, port.ErrRecordVersionConflict
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1}))

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestClaimTask_ConnectorTypedTask_NotHumanActionable(t *testing.T) {
	fake := &fakeTaskService{
		claim: func(_ context.Context, _, _, _ uuid.UUID, _ int64) (*port.Task, error) {
			return nil, port.ErrTaskNotHumanActionable
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1}))

	assert.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "TASK_NOT_HUMAN_ACTIONABLE", body.Code)
}

func TestClaimTask_MissingRecordVersion(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCompleteTask_NotAssignee(t *testing.T) {
	fake := &fakeTaskService{
		complete: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
			return nil, port.ErrNotAssignee
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/complete", map[string]any{
		"result_json":    map[string]any{"decision": "approve"},
		"record_version": 2,
	}))

	require.Equal(t, http.StatusForbidden, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "NOT_ASSIGNEE", body.Code)
}

func TestDeferTask(t *testing.T) {
	fake := &fakeTaskService{
		deferTask: func(_ context.Context, _, _, _ uuid.UUID, reason string, _ int64) (*port.Task, error) {
			assert.Equal(t, "backlog", reason)
			return newTestTask(), nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/defer", map[string]any{
		"reason": "backlog", "record_version": 4,
	}))

	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestReassignTask(t *testing.T) {
	newUser := uuid.New()
	fake := &fakeTaskService{
		reassign: func(_ context.Context, _, _, actorUserID, newUserID uuid.UUID, _ int64) (*port.Task, error) {
			assert.Equal(t, testUserID, actorUserID)
			assert.Equal(t, newUser, newUserID)
			return newTestTask(), nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", map[string]any{
		"new_user_id": newUser, "record_version": 4,
	})))

	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestReassignTask_NotAdmin_Forbidden(t *testing.T) {
	fake := &fakeTaskService{
		reassign: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			t.Fatal("must not call the service when the caller is not admin")
			return nil, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	require.Equal(t, http.StatusForbidden, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "FORBIDDEN", body.Code)
}

func TestListActiveByUser(t *testing.T) {
	targetUser := uuid.New()
	fake := &fakeTaskService{
		activeByUser: func(_ context.Context, tenantID, userID uuid.UUID, _ port.Page) (port.PageResult[*port.ActiveUserTask], error) {
			assert.Equal(t, testTenantID, tenantID)
			assert.Equal(t, targetUser, userID)
			return port.PageResult[*port.ActiveUserTask]{Items: []*port.ActiveUserTask{{
				TaskID: testTaskID, WorkflowInstanceID: testInstID, UserID: targetUser, Status: port.TaskStatusReady,
			}}}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+targetUser.String(), nil)))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []map[string]any `json:"items"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Len(t, body.Items, 1)
}

func TestListActiveByUser_NotAdmin_Forbidden(t *testing.T) {
	fake := &fakeTaskService{
		activeByUser: func(context.Context, uuid.UUID, uuid.UUID, port.Page) (port.PageResult[*port.ActiveUserTask], error) {
			t.Fatal("must not call the service when the caller is not admin")
			return port.PageResult[*port.ActiveUserTask]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+uuid.New().String(), nil))

	require.Equal(t, http.StatusForbidden, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "FORBIDDEN", body.Code)
}

func TestListActiveByUser_MissingUserID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodGet, "/api/v1/workflows/active-by-user", nil)))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListActiveByUser_InvalidLimit_Rejected(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+uuid.New().String()+"&limit=0", nil)))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListActiveByUser_InvalidCursor_Rejected(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+uuid.New().String()+"&cursor=not-a-valid-cursor", nil)))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUnauthenticatedRequest(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Del("x-tenant-id")
	r.Header.Del("x-user-id")
	w := do(router, r)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
