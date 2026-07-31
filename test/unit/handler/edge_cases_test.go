package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// --- callerIdentity edge cases ---

func TestCallerIdentity_MalformedTenantID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-tenant-id", "not-a-uuid")
	w := do(router, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestCallerIdentity_MalformedUserID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	r := req(http.MethodGet, "/api/v1/tasks", nil)
	r.Header.Set("x-user-id", "not-a-uuid")
	w := do(router, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- path/query parsing edge cases ---

func TestGetTask_InvalidID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/not-a-uuid", nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListTasks_InvalidInstanceIDFilter(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?instance_id=not-a-uuid", nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListTasks_StatusAndInstanceFilterAndPaging(t *testing.T) {
	instID := uuid.New()
	deptID := uuid.New()
	var gotFilter port.TaskFilter
	var gotPage port.Page
	fake := &fakeTaskService{
		list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, filter port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
			gotFilter = filter
			gotPage = page
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?status=READY&instance_id="+instID.String()+"&department_id="+deptID.String()+"&limit=500&cursor=abc", nil))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotFilter.Status)
	assert.Equal(t, port.TaskStatusReady, *gotFilter.Status)
	require.NotNil(t, gotFilter.WorkflowInstanceID)
	assert.Equal(t, instID, *gotFilter.WorkflowInstanceID)
	require.NotNil(t, gotFilter.DepartmentID)
	assert.Equal(t, deptID, *gotFilter.DepartmentID)
	assert.Equal(t, 100, gotPage.Limit, "limit clamps to the 100 max")
	assert.Equal(t, "abc", gotPage.Cursor)
}

func TestListTasks_InvalidDepartmentIDFilter(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?department_id=not-a-uuid", nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTask_UpstreamTimeout(t *testing.T) {
	fake := &fakeTaskService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			return nil, nil, context.DeadlineExceeded
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", body.Code)
}

func TestListTasks_DefaultLimit(t *testing.T) {
	var gotPage port.Page
	fake := &fakeTaskService{
		list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, _ port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
			gotPage = page
			return port.PageResult[*port.Task]{}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks?limit=not-a-number", nil))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 25, gotPage.Limit)
}

func TestListTasks_ServiceError(t *testing.T) {
	fake := &fakeTaskService{
		list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
			return port.PageResult[*port.Task]{}, port.ErrNotAuthorizedForRead
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks", nil))

	assert.Equal(t, http.StatusForbidden, w.Code)
}

// --- toTaskResp / toTaskAssignmentResp optional-field branches ---

func TestGetTask_WithCompletedAndVacatedTimestamps(t *testing.T) {
	completedAt := time.Now()
	vacatedAt := time.Now()
	fake := &fakeTaskService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			task := newTestTask()
			task.CompletedAt = &completedAt
			return task, []*port.TaskAssignment{{ID: uuid.New(), UserID: testUserID, VacatedAt: &vacatedAt}}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/tasks/"+testTaskID.String(), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		CompletedAt *string          `json:"completed_at"`
		Assignments []map[string]any `json:"assignments"`
	}
	decodeJSON(t, w.Body, &body)
	require.NotNil(t, body.CompletedAt)
	require.Len(t, body.Assignments, 1)
	assert.NotEmpty(t, body.Assignments[0]["vacated_at"])
}

// --- mapErr's remaining branches ---

func TestClaimTask_TaskAlreadyClaimed(t *testing.T) {
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return nil, port.ErrTaskAlreadyClaimed
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1}))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "TASK_ALREADY_CLAIMED", body.Code)
}

func TestClaimTask_ClaimNotApplicable(t *testing.T) {
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return nil, port.ErrClaimNotApplicable
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1}))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "CLAIM_NOT_APPLICABLE", body.Code)
}

func TestDeferTask_InvalidTaskState(t *testing.T) {
	fake := &fakeTaskService{
		deferTask: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error) {
			return nil, port.ErrInvalidTaskState
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/defer", map[string]any{"record_version": 1}))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INVALID_TASK_STATE", body.Code)
}

func TestReassignTask_UnknownError(t *testing.T) {
	fake := &fakeTaskService{
		reassign: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return nil, assertUnknownErr
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", map[string]any{
		"new_user_id": uuid.New(), "record_version": 1,
	}))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
}

var assertUnknownErr = errUnexpected{}

type errUnexpected struct{}

func (errUnexpected) Error() string { return "unexpected failure" }

func TestOverrideNodeAssignee_OverrideNoOp(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusReady, RecordVersion: 4}, nil
		},
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, port.ErrOverrideNoOp
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	require.Equal(t, http.StatusBadRequest, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "OVERRIDE_NO_OP", body.Code)
}

func TestOverrideNodeAssignee_GetByNodeError(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return nil, port.ErrTaskNotFound
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestOverrideNodeAssignee_SignalFailure(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusReady, RecordVersion: 4}, nil
		},
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return &port.AssigneeOverride{RecordVersion: 5}, nil
		},
		signalReassign: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) error {
			return assertUnknownErr
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	// Persist already committed; the signal failing afterward is the accepted
	// residual gap (LLD Appendix B) — the response reports the failure, but
	// no compensating rollback is attempted.
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestOverrideNodeAssignee_InvalidBody(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- remaining bind/error-path gaps across the signal-only handlers ---

func TestCompleteTask_MissingRecordVersion(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/complete", map[string]any{
		"result_json": map[string]any{"decision": "approve"},
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCompleteTask_RecordVersionConflict(t *testing.T) {
	fake := &fakeTaskService{
		complete: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
			return nil, port.ErrRecordVersionConflict
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/complete", map[string]any{
		"record_version": 1,
	}))

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestClaimTask_InvalidID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/not-a-uuid/claim", map[string]any{"record_version": 1}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDeferTask_MissingRecordVersion(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/defer", map[string]any{}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDeferTask_InvalidID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/not-a-uuid/defer", map[string]any{"record_version": 1}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestReassignTask_MissingFields(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", map[string]any{}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestReassignTask_InvalidID(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, "/api/v1/tasks/not-a-uuid/reassign", map[string]any{
		"new_user_id": uuid.New(), "record_version": 1,
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListActiveByUser_ServiceError(t *testing.T) {
	fake := &fakeTaskService{
		activeByUser: func(context.Context, uuid.UUID, uuid.UUID, port.Page) (port.PageResult[*port.ActiveUserTask], error) {
			return port.PageResult[*port.ActiveUserTask]{}, assertUnknownErr
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+uuid.New().String(), nil))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- request-body floor: 413/415 on every mutating endpoint (LLD §5.10) ---

func TestClaimTask_UnsupportedMediaType(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	r := req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1})
	r.Header.Set("Content-Type", "text/plain")
	w := do(router, r)

	require.Equal(t, http.StatusUnsupportedMediaType, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", body.Code)
}

func TestDeferTask_PayloadTooLarge(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	oversized := strings.Repeat("a", 11<<20) // over the 10 MB cap
	w := do(router, req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/defer", map[string]any{
		"reason": oversized, "record_version": 1,
	}))

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "PAYLOAD_TOO_LARGE", body.Code)
}

func TestOverrideNodeAssignee_UnsupportedMediaType(t *testing.T) {
	router := newRouter(newHandler(&fakeTaskService{}, &fakeEligibilityChecker{}))

	r := req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})
	r.Header.Set("Content-Type", "application/xml")
	w := do(router, r)

	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}
