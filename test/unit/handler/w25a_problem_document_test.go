package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w25aErrBase = "https://errors.bcbp.io/execution/"

// w25aFailing returns task and instance fakes whose every method fails with err.
func w25aFailing(err error) (*fakeTaskService, *fakeInstanceService) {
	tasks := &fakeTaskService{
		list: func(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
			return port.PageResult[*port.Task]{}, err
		},
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
			return nil, nil, err
		},
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) { return nil, err },
		complete: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
			return nil, err
		},
		deferTask: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error) {
			return nil, err
		},
		reassign: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return nil, err
		},
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) { return nil, err },
		activeByUser: func(context.Context, uuid.UUID, uuid.UUID, port.Page) (port.PageResult[*port.ActiveUserTask], error) {
			return port.PageResult[*port.ActiveUserTask]{}, err
		},
	}
	instances := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) { return nil, err },
		list: func(context.Context, uuid.UUID, port.ReadScope, port.InstanceFilter, port.Page) (port.PageResult[*port.Instance], error) {
			return port.PageResult[*port.Instance]{}, err
		},
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Instance, []*port.Task, error) {
			return nil, nil, err
		},
		pause:        func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error { return err },
		resume:       func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error { return err },
		cancel:       func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error { return err },
		terminate:    func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error { return err },
		forceForward: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error { return err },
		forceBack:    func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error { return err },
		listEvents: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, port.Page) (port.PageResult[*port.WorkflowEvent], error) {
			return port.PageResult[*port.WorkflowEvent]{}, err
		},
		answerOutsideWait: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, string, map[string]any) error { return err },
	}
	return tasks, instances
}

// Every error a route answers is one problem document: application/problem+json, status equal to the HTTP
// status, the instance field is the request path, and the type, title, code and detail are the catalogue's for the
// error; the wrapped cause of a refusal stays out of the body.
func TestRoutes_ErrorAnswers_AreOneProblemDocumentPerRoute(t *testing.T) {
	inst := testInstID.String()
	task := testTaskID.String()
	version := `"record_version":1`
	newUser := `"new_user_id":"` + uuid.NewString() + `"`
	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		admin      bool
		err        error
		wantStatus int
		wantCode   string
		wantTitle  string
		wantType   string
		wantDetail string
	}{
		{"get task", http.MethodGet, "/api/v1/tasks/" + task, "", false, port.ErrTaskNotFound, 404, "TASK_NOT_FOUND", "Task Not Found", "task-not-found", "task not found"},
		{"list tasks", http.MethodGet, "/api/v1/tasks", "", false, port.ErrNotAuthorizedForRead, 403, "NOT_AUTHORIZED_FOR_RESOURCE", "Not Authorized For Resource", "not-authorized-for-resource", "caller is not authorized to read this resource"},
		{"claim task", http.MethodPost, "/api/v1/tasks/" + task + "/claim", `{` + version + `}`, false, port.ErrTaskAlreadyClaimed, 409, "TASK_ALREADY_CLAIMED", "Task Already Claimed", "task-already-claimed", "task already claimed by another assignee"},
		{"complete task", http.MethodPost, "/api/v1/tasks/" + task + "/complete", `{` + version + `}`, false, port.ErrInvalidTaskState, 409, "INVALID_TASK_STATE", "Invalid Task State", "invalid-task-state", "action not valid for task's current status"},
		{"defer task", http.MethodPost, "/api/v1/tasks/" + task + "/defer", `{"reason":"r",` + version + `}`, false, port.ErrRecordVersionConflict, 409, "RECORD_VERSION_CONFLICT", "Record Version Conflict", "record-version-conflict", "record version conflict"},
		{"reassign task", http.MethodPost, "/api/v1/tasks/" + task + "/reassign", `{` + newUser + `,` + version + `}`, true, port.ErrNotAssignee, 403, "NOT_ASSIGNEE", "Not Assigned", "not-assignee", "caller is not the task's active assignee"},
		{"active by user", http.MethodGet, "/api/v1/workflows/active-by-user?user_id=" + uuid.NewString(), "", true, context.DeadlineExceeded, 503, "UPSTREAM_UNAVAILABLE", "Service Unavailable", "upstream-unavailable", "request timed out"},
		{"override node assignee", http.MethodPost, "/api/v1/instances/" + inst + "/nodes/finance%2Freview/override", `{` + newUser + `,` + version + `}`, true, port.ErrOverrideNoOp, 400, "OVERRIDE_NO_OP", "Override No-Op", "override-no-op", "new assignee is already the current assignee"},
		{"complete outside wait", http.MethodPost, "/api/v1/instances/" + inst + "/nodes/Wait_1/complete", `{}`, false, port.ErrWaitNotFound, 404, "WAIT_NOT_FOUND", "Wait Not Found", "wait-not-found", "not an outside wait of the instance's plan"},
		{"fire boundary", http.MethodPost, "/api/v1/instances/" + inst + "/nodes/Wait_1/boundaries/Timer_1/fire", `{}`, false, port.ErrNotWaiting, 409, "NOT_WAITING", "Not Waiting", "not-waiting", "instance is not waiting there"},
		{"start instance", http.MethodPost, "/api/v1/instances", `{"subject_id":"SUBJ-1","business_key":"TND-001","workflow_version_id":"` + uuid.NewString() + `"}`, false, port.ErrDuplicateBusinessKey, 409, "DUPLICATE_BUSINESS_KEY", "Duplicate Business Key", "duplicate-business-key", "business key already active for this subject"},
		{"list instances", http.MethodGet, "/api/v1/instances", "", false, context.DeadlineExceeded, 503, "UPSTREAM_UNAVAILABLE", "Service Unavailable", "upstream-unavailable", "request timed out"},
		{"get instance", http.MethodGet, "/api/v1/instances/" + inst, "", false, port.ErrInstanceNotFound, 404, "INSTANCE_NOT_FOUND", "Instance Not Found", "instance-not-found", "instance not found"},
		{"list instance events", http.MethodGet, "/api/v1/instances/" + inst + "/events", "", false, port.ErrInstanceNotFound, 404, "INSTANCE_NOT_FOUND", "Instance Not Found", "instance-not-found", "instance not found"},
		{"pause instance", http.MethodPost, "/api/v1/instances/" + inst + "/pause", `{` + version + `}`, true, port.ErrInvalidInstanceState, 409, "INVALID_INSTANCE_STATE", "Invalid Instance State", "invalid-instance-state", "action not valid for instance's current status"},
		{"resume instance", http.MethodPost, "/api/v1/instances/" + inst + "/resume", `{` + version + `}`, true, port.ErrInvalidInstanceState, 409, "INVALID_INSTANCE_STATE", "Invalid Instance State", "invalid-instance-state", "action not valid for instance's current status"},
		{"cancel instance", http.MethodPost, "/api/v1/instances/" + inst + "/cancel", `{"reason":"r",` + version + `}`, true, port.ErrInstanceAlreadyTerminal, 409, "INSTANCE_ALREADY_TERMINAL", "Instance Already Terminal", "instance-already-terminal", "instance is already in a terminal state"},
		{"terminate instance", http.MethodPost, "/api/v1/instances/" + inst + "/terminate", `{"reason":"r"}`, true, port.ErrInstanceAlreadyTerminal, 409, "INSTANCE_ALREADY_TERMINAL", "Instance Already Terminal", "instance-already-terminal", "instance is already in a terminal state"},
		{"force-forward instance", http.MethodPost, "/api/v1/instances/" + inst + "/force-forward", `{"target_node_key":"dept/n",` + version + `}`, true, fmt.Errorf("%w: dept/n is not in the plan", port.ErrTargetNodeNotFound), 409, "TARGET_NODE_NOT_FOUND", "Target Node Not Found", "target-node-not-found", "target node not found in the compiled plan: dept/n is not in the plan"},
		{"force-back instance", http.MethodPost, "/api/v1/instances/" + inst + "/force-back", `{` + version + `}`, true, port.ErrForceBackNoSavedBranch, 409, "FORCE_BACK_NO_SAVED_BRANCH", "Force Back No Saved Branch", "force-back-no-saved-branch", "no completed step to go back to"},
		{"wrapped refusal hides its cause", http.MethodGet, "/api/v1/tasks/" + task, "", false, fmt.Errorf("select task row 77 in tenant t-9: %w", port.ErrTaskNotFound), 404, "TASK_NOT_FOUND", "Task Not Found", "task-not-found", "task not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks, instances := w25aFailing(tc.err)
			router := newRouter(handler.New(handler.Services{Tasks: tasks, Instances: instances, Eligibility: &fakeEligibilityChecker{}}))
			r := w25aRaw(tc.method, tc.path, "application/json", tc.body)
			if tc.admin {
				r.Header.Set("x-tenant-roles", "tenant_admin")
			}

			w := w25aServe(router, r)

			p := w25aDecodeProblem(t, w, tc.wantStatus)
			assert.Equal(t, tc.wantStatus, p.Status)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantTitle, p.Title)
			assert.Equal(t, w25aErrBase+tc.wantType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			wantInstance, _, _ := strings.Cut(tc.path, "?")
			assert.Equal(t, w25aDecodedPath(t, wantInstance), p.Instance)
			assert.NotContains(t, w.Body.String(), "row 77")
		})
	}
}

// w25aDecodedPath returns the path as the router reports it in the instance field (percent escapes decoded).
func w25aDecodedPath(t *testing.T, path string) string {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, err)
	return r.URL.Path
}

// An id path parameter that is not a UUID is a problem document too: 400 BAD_REQUEST naming id.
func TestRoutes_InvalidIDPathParameter_IsAProblemDocumentNamingID(t *testing.T) {
	tasks, instances := w25aFailing(nil)
	router := newRouter(handler.New(handler.Services{Tasks: tasks, Instances: instances, Eligibility: &fakeEligibilityChecker{}}))

	w := w25aServe(router, w25aRaw(http.MethodGet, "/api/v1/instances/not-a-uuid", "", ""))

	p := w25aDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, w25aErrBase+"bad-request", p.Type)
	assert.Equal(t, "Bad Request", p.Title)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, "invalid id path parameter", p.Detail)
	assert.Equal(t, "/api/v1/instances/not-a-uuid", p.Instance)
	assert.Equal(t, []w25aParam{{"id", "is not valid"}}, w25aParams(p))
}
