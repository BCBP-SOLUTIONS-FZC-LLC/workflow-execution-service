package handler_test

import (
	"context"
	"encoding/json"
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

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-departments", "dept-a, dept-b")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testUserID, gotScope.CallerUserID)
	assert.Equal(t, []string{"dept-a", "dept-b"}, gotScope.Departments)
	assert.False(t, gotScope.IsAdmin)

	var body struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Len(t, body.Items, 1)
	assert.Equal(t, "next", body.NextCursor)
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

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", map[string]any{
		"new_user_id": newUser, "record_version": 4,
	}))

	assert.Equal(t, http.StatusAccepted, w.Code)
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

	w := do(router, req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+targetUser.String(), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []map[string]any `json:"items"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Len(t, body.Items, 1)
}

func TestListActiveByUser_MissingUserID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/workflows/active-by-user", nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListActiveByUser_InvalidLimit_Rejected(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+uuid.New().String()+"&limit=0", nil))

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
