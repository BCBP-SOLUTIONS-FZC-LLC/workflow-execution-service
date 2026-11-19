package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const (
	w21aInvalidFieldsDetail = "request body has missing or invalid fields"
	w21aWrongTypeDetail     = "request body has a field of the wrong type"
	w21aSignalDuration      = "workflow_execution_service_task_signal_duration_seconds"
	w21aStartDuration       = "workflow_execution_service_instance_start_duration_seconds"
)

type w21aTaskCall struct {
	op        string
	tenantID  uuid.UUID
	taskID    uuid.UUID
	userID    uuid.UUID
	newUserID uuid.UUID
	version   int64
	text      string
	result    string
}

// w21aTaskActions fakes the task service; err, when set, is what every action answers.
type w21aTaskActions struct {
	calls []w21aTaskCall
	task  *port.Task
	err   error
}

func (a *w21aTaskActions) service() *fakeTaskService {
	answer := func() (*port.Task, error) {
		if a.err != nil {
			return nil, a.err
		}
		return a.task, nil
	}
	return &fakeTaskService{
		complete: func(_ context.Context, tenantID, taskID, userID uuid.UUID, result json.RawMessage, version int64) (*port.Task, error) {
			a.calls = append(a.calls, w21aTaskCall{op: "complete", tenantID: tenantID, taskID: taskID, userID: userID, version: version, result: string(result)})
			return answer()
		},
		deferTask: func(_ context.Context, tenantID, taskID, userID uuid.UUID, reason string, version int64) (*port.Task, error) {
			a.calls = append(a.calls, w21aTaskCall{op: "defer", tenantID: tenantID, taskID: taskID, userID: userID, version: version, text: reason})
			return answer()
		},
		reassign: func(_ context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, version int64) (*port.Task, error) {
			a.calls = append(a.calls, w21aTaskCall{op: "reassign", tenantID: tenantID, taskID: taskID, userID: actorUserID, newUserID: newUserID, version: version})
			return answer()
		},
	}
}

func w21aNewTaskActions() (*w21aTaskActions, http.Handler) {
	actions := &w21aTaskActions{task: newTestTask()}
	return actions, newRouter(newHandler(actions.service(), &fakeEligibilityChecker{}))
}

func TestW21aCompleteTask_ResultJSONOmitted_ReachesTheServiceAsNothing(t *testing.T) {
	actions, router := w21aNewTaskActions()

	w := w21aServe(router, http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/complete", `{"record_version":3}`, false, nil)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, []w21aTaskCall{{op: "complete", tenantID: testTenantID, taskID: testTaskID, userID: testUserID, version: 3, result: ""}}, actions.calls,
		"no result_json is an empty result, not the text null or {}")
	assert.Equal(t, testTaskID.String(), w21aString(t, w21aObject(t, w.Body.Bytes())["id"]))
}

func TestW21aTaskSignals_RefusedAndFailedRequests_RecordNoDuration(t *testing.T) {
	task := "/api/v1/tasks/" + testTaskID.String()
	longReason := strings.Repeat("a", 501)
	cases := []struct {
		name       string
		op         string
		path       string
		body       string
		serviceErr error
		wantStatus int
		wantCalled bool
		wantSample bool
	}{
		{name: "complete succeeds", op: "complete", path: task + "/complete", body: `{"record_version":3}`, wantStatus: http.StatusAccepted, wantCalled: true, wantSample: true},
		{name: "complete finds a newer version", op: "complete", path: task + "/complete", body: `{"record_version":3}`,
			serviceErr: fmt.Errorf("complete: %w", port.ErrRecordVersionConflict), wantStatus: http.StatusConflict, wantCalled: true},
		{name: "complete times out", op: "complete", path: task + "/complete", body: `{"record_version":3}`,
			serviceErr: context.DeadlineExceeded, wantStatus: http.StatusServiceUnavailable, wantCalled: true},
		{name: "complete without a version", op: "complete", path: task + "/complete", body: `{}`, wantStatus: http.StatusBadRequest},
		{name: "complete with a result that is not an object", op: "complete", path: task + "/complete", body: `{"result_json":[1],"record_version":3}`, wantStatus: http.StatusBadRequest},
		{name: "complete with an id that is not a uuid", op: "complete", path: "/api/v1/tasks/not-a-uuid/complete", body: `{"record_version":3}`, wantStatus: http.StatusBadRequest},
		{name: "defer succeeds", op: "defer", path: task + "/defer", body: `{"reason":"later","record_version":3}`, wantStatus: http.StatusOK, wantCalled: true, wantSample: true},
		{name: "defer finds a newer version", op: "defer", path: task + "/defer", body: `{"reason":"later","record_version":3}`,
			serviceErr: fmt.Errorf("defer: %w", port.ErrRecordVersionConflict), wantStatus: http.StatusConflict, wantCalled: true},
		{name: "defer by someone who is not the assignee", op: "defer", path: task + "/defer", body: `{"reason":"later","record_version":3}`,
			serviceErr: port.ErrNotAssignee, wantStatus: http.StatusForbidden, wantCalled: true},
		{name: "defer without a version", op: "defer", path: task + "/defer", body: `{"reason":"later"}`, wantStatus: http.StatusBadRequest},
		{name: "defer with a reason over the limit", op: "defer", path: task + "/defer", body: `{"reason":"` + longReason + `","record_version":3}`, wantStatus: http.StatusBadRequest},
		{name: "defer with an id that is not a uuid", op: "defer", path: "/api/v1/tasks/not-a-uuid/defer", body: `{"record_version":3}`, wantStatus: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actions, router := w21aNewTaskActions()
			actions.err = tc.serviceErr
			labels := map[string]string{"operation": tc.op}
			before := obstest.Sum(t, w21aSignalDuration, labels)

			w := w21aServe(router, http.MethodPost, tc.path, tc.body, false, nil)

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			wantDelta := 0.0
			if tc.wantSample {
				wantDelta = 1
			}
			assert.Equal(t, before+wantDelta, obstest.Sum(t, w21aSignalDuration, labels), "only an answered signal is observed")
			if tc.wantCalled {
				require.Len(t, actions.calls, 1)
				assert.Equal(t, tc.op, actions.calls[0].op)
				assert.Equal(t, testTenantID, actions.calls[0].tenantID)
				assert.Equal(t, testTaskID, actions.calls[0].taskID)
				assert.Equal(t, testUserID, actions.calls[0].userID)
				assert.EqualValues(t, 3, actions.calls[0].version)
			} else {
				assert.Empty(t, actions.calls, "a refused request must not reach the service")
			}
		})
	}
}

func TestW21aDeferTask_ReasonLength_IsCountedInCharactersAndRefusedOverFiveHundred(t *testing.T) {
	cases := []struct {
		name       string
		reason     string
		wantStatus int
	}{
		{"500 ASCII characters", strings.Repeat("a", 500), http.StatusOK},
		{"500 characters that take two bytes each", strings.Repeat("é", 500), http.StatusOK},
		{"501 ASCII characters", strings.Repeat("a", 501), http.StatusBadRequest},
		{"501 characters that take two bytes each", strings.Repeat("é", 501), http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actions, router := w21aNewTaskActions()
			body, err := json.Marshal(map[string]any{"reason": tc.reason, "record_version": 3})
			require.NoError(t, err)

			w := w21aServe(router, http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/defer", string(body), false, nil)

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			if tc.wantStatus == http.StatusOK {
				assert.Equal(t, []w21aTaskCall{{op: "defer", tenantID: testTenantID, taskID: testTaskID, userID: testUserID, version: 3, text: tc.reason}}, actions.calls)
				return
			}
			p := w21aDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w21aBadRequestType, p.Type)
			assert.Equal(t, w21aInvalidFieldsDetail, p.Detail)
			assert.Equal(t, []w21aParam{{Name: "reason", Reason: "is not valid"}}, p.InvalidParams)
			assert.NotContains(t, w.Body.String(), "Key: ", "the validator's own text stays out of the answer")
			assert.Empty(t, actions.calls, "a refused request must not reach the service")
		})
	}
}

func TestW21aDeferTask_IdNotAUUID_IsRefusedNamingThePathParameter(t *testing.T) {
	for _, id := range []string{"not-a-uuid", "42", "33333333-3333-3333-3333-33333333333"} {
		t.Run(id, func(t *testing.T) {
			actions, router := w21aNewTaskActions()

			w := w21aServe(router, http.MethodPost, "/api/v1/tasks/"+id+"/defer", `{"reason":"later","record_version":3}`, false, nil)

			p := w21aDecodeProblem(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w21aBadRequestType, p.Type)
			assert.Equal(t, "invalid id path parameter", p.Detail)
			assert.Equal(t, []w21aParam{{Name: "id", Reason: "is not valid"}}, p.InvalidParams)
			assert.Empty(t, actions.calls, "a refused request must not reach the service")
		})
	}
}

func TestW21aReassignTask_ByAnAdmin_AnswersTheTaskSummary(t *testing.T) {
	actions, router := w21aNewTaskActions()
	newUser := uuid.New()
	task := newFullyPopulatedTask()
	task.TaskType = "userTask"
	task.AssigneeCount = 2
	actions.task = task

	w := w21aServe(router, http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", `{"new_user_id":"`+newUser.String()+`","record_version":4}`, true, nil)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, []w21aTaskCall{{op: "reassign", tenantID: testTenantID, taskID: testTaskID, userID: testUserID, newUserID: newUser, version: 4}}, actions.calls)
	body := w21aObject(t, w.Body.Bytes())
	assert.Equal(t, task.ID.String(), w21aString(t, body["id"]))
	assert.Equal(t, task.WorkflowInstanceID.String(), w21aString(t, body["workflow_instance_id"]))
	assert.Equal(t, testTenantID.String(), w21aString(t, body["tenant_id"]))
	assert.Equal(t, "review_finance", w21aString(t, body["node_key"]))
	assert.Equal(t, "userTask", w21aString(t, body["task_type"]))
	assert.Equal(t, task.DepartmentID.String(), w21aString(t, body["department_id"]))
	assert.Equal(t, "READY", w21aString(t, body["status"]))
	assert.EqualValues(t, 4, w21aNumber(t, body["record_version"]))
	assert.Equal(t, "single", w21aString(t, body["assignee_mode"]))
	assert.EqualValues(t, 2, w21aNumber(t, body["assignee_count"]))
	assert.True(t, task.CreatedAt.Equal(w21aTime(t, body["created_at"])), "created_at is the task's own")
	assert.True(t, task.DueAt.Equal(w21aTime(t, body["due_at"])), "due_at is the task's own")
	assert.Equal(t, task.DeferredFromTaskID.String(), w21aString(t, body["deferred_from_task_id"]))
	assert.NotContains(t, body, "completed_at", "completed_at is left out while unset")
}

func TestW21aReassignTask_StaleRecordVersion_IsAnswered409AndTheVersionReachedTheService(t *testing.T) {
	actions, router := w21aNewTaskActions()
	actions.err = fmt.Errorf("reassign task: %w", port.ErrRecordVersionConflict)
	newUser := uuid.New()

	w := w21aServe(router, http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", `{"new_user_id":"`+newUser.String()+`","record_version":2}`, true, nil)

	p := w21aDecodeProblem(t, w)
	require.Equal(t, http.StatusConflict, w.Code)
	assert.Equal(t, "RECORD_VERSION_CONFLICT", p.Code)
	assert.Equal(t, []w21aTaskCall{{op: "reassign", tenantID: testTenantID, taskID: testTaskID, userID: testUserID, newUserID: newUser, version: 2}}, actions.calls,
		"the version the caller sent is the one the service compared")
}

type w21aStartCall struct {
	tenantID, userID, versionID uuid.UUID
	subject, businessKey        string
	context                     string
	overrides                   map[string]uuid.UUID
}

// w21aStarter fakes the instance service's Start; err, when set, is the answer.
type w21aStarter struct {
	calls []w21aStartCall
	err   error
}

func w21aNewStarter() (*w21aStarter, http.Handler) {
	starter := &w21aStarter{}
	instances := &fakeInstanceService{start: func(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
		starter.calls = append(starter.calls, w21aStartCall{
			tenantID: in.TenantID, userID: in.StartedByUserID, versionID: in.WorkflowVersionID,
			subject: in.SubjectID, businessKey: in.BusinessKey, context: string(in.ContextJSON), overrides: in.OverrideMap,
		})
		if starter.err != nil {
			return nil, starter.err
		}
		return &port.Instance{ID: testInstID, TenantID: in.TenantID, SubjectID: in.SubjectID, BusinessKey: in.BusinessKey, WorkflowVersionID: in.WorkflowVersionID}, nil
	}}
	return starter, newRouter(newInstanceHandler(instances))
}

func TestW21aStartInstance_ContextNullOrEmptyObject_IsAccepted(t *testing.T) {
	versionID := uuid.New()
	cases := []struct {
		name  string
		extra string
	}{
		{"context_json null", `,"context_json":null`},
		{"context_json an empty object", `,"context_json":{}`},
		{"context_json omitted", ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			starter, router := w21aNewStarter()
			body := `{"subject_id":"subj-1","business_key":"bk-1","workflow_version_id":"` + versionID.String() + `"` + tc.extra + `}`

			w := w21aServe(router, http.MethodPost, "/api/v1/instances", body, false, nil)

			require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
			require.Len(t, starter.calls, 1)
			call := starter.calls[0]
			assert.Equal(t, testTenantID, call.tenantID)
			assert.Equal(t, testUserID, call.userID)
			assert.Equal(t, versionID, call.versionID)
			assert.Equal(t, "subj-1", call.subject)
			assert.Equal(t, "bk-1", call.businessKey)
			var entries map[string]any
			if call.context != "" {
				require.NoError(t, json.Unmarshal([]byte(call.context), &entries), "what reaches the service is empty or an object")
			}
			assert.Empty(t, entries, "no context entries are invented")
			assert.Equal(t, testInstID.String(), w21aString(t, w21aObject(t, w.Body.Bytes())["id"]))
		})
	}
}

func TestW21aStartInstance_RefusedAndFailedRequests_RecordNoStartDuration(t *testing.T) {
	versionID := uuid.New()
	valid := `{"subject_id":"subj-1","business_key":"bk-1","workflow_version_id":"` + versionID.String() + `"}`
	cases := []struct {
		name       string
		body       string
		serviceErr error
		wantStatus int
		wantCalled bool
		wantSample bool
	}{
		{name: "a start that succeeds", body: valid, wantStatus: http.StatusCreated, wantCalled: true, wantSample: true},
		{name: "a duplicate business key", body: valid, serviceErr: fmt.Errorf("start: %w", port.ErrDuplicateBusinessKey), wantStatus: http.StatusConflict, wantCalled: true},
		{name: "an unknown version", body: valid, serviceErr: port.ErrVersionNotFound, wantStatus: http.StatusNotFound, wantCalled: true},
		{name: "a timeout", body: valid, serviceErr: context.DeadlineExceeded, wantStatus: http.StatusServiceUnavailable, wantCalled: true},
		{name: "an unexpected failure", body: valid, serviceErr: errors.New("boom"), wantStatus: http.StatusInternalServerError, wantCalled: true},
		{name: "a missing subject_id", body: `{"business_key":"bk-1","workflow_version_id":"` + versionID.String() + `"}`, wantStatus: http.StatusBadRequest},
		{name: "a subject_id of the wrong type", body: `{"subject_id":7,"business_key":"bk-1","workflow_version_id":"` + versionID.String() + `"}`, wantStatus: http.StatusBadRequest},
		{name: "a context that is not an object", body: `{"subject_id":"subj-1","business_key":"bk-1","workflow_version_id":"` + versionID.String() + `","context_json":[1]}`, wantStatus: http.StatusBadRequest},
		{name: "a body that is not JSON", body: `nope`, wantStatus: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			starter, router := w21aNewStarter()
			starter.err = tc.serviceErr
			before := obstest.Sum(t, w21aStartDuration, nil)

			w := w21aServe(router, http.MethodPost, "/api/v1/instances", tc.body, false, nil)

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			if tc.wantStatus == http.StatusInternalServerError {
				assert.NotContains(t, w.Body.String(), "boom", "the service's own error text stays out of the answer")
				assert.Equal(t, "an unexpected error occurred", w21aDecodeProblem(t, w).Detail)
			}
			wantDelta := 0.0
			if tc.wantSample {
				wantDelta = 1
			}
			assert.Equal(t, before+wantDelta, obstest.Sum(t, w21aStartDuration, nil), "only a 201 is observed")
			if tc.wantCalled {
				require.Len(t, starter.calls, 1)
				assert.Equal(t, testTenantID, starter.calls[0].tenantID)
				assert.Equal(t, testUserID, starter.calls[0].userID)
				assert.Equal(t, versionID, starter.calls[0].versionID)
			} else {
				assert.Empty(t, starter.calls, "a refused request must not reach the service")
			}
		})
	}
}

func TestW21aStartInstance_FieldOfTheWrongJSONType_IsRefusedNamingTheField(t *testing.T) {
	versionID := uuid.New()
	version := `"workflow_version_id":"` + versionID.String() + `"`
	cases := []struct {
		name      string
		body      string
		wantField string
	}{
		{"subject_id as a number", `{"subject_id":123,"business_key":"bk-1",` + version + `}`, "subject_id"},
		{"subject_id as an array", `{"subject_id":["a"],"business_key":"bk-1",` + version + `}`, "subject_id"},
		{"subject_id as an object", `{"subject_id":{"a":1},"business_key":"bk-1",` + version + `}`, "subject_id"},
		{"business_key as a number", `{"subject_id":"subj-1","business_key":42,` + version + `}`, "business_key"},
		{"business_key as an array", `{"subject_id":"subj-1","business_key":["a"],` + version + `}`, "business_key"},
		{"business_key as a boolean", `{"subject_id":"subj-1","business_key":true,` + version + `}`, "business_key"},
		{"override_map as an array", `{"subject_id":"subj-1","business_key":"bk-1",` + version + `,"override_map":["a"]}`, "override_map"},
		{"override_map as a string", `{"subject_id":"subj-1","business_key":"bk-1",` + version + `,"override_map":"a"}`, "override_map"},
		{"workflow_version_id as a number", `{"subject_id":"subj-1","business_key":"bk-1","workflow_version_id":5}`, "workflow_version_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			starter, router := w21aNewStarter()

			w := w21aServe(router, http.MethodPost, "/api/v1/instances", tc.body, false, nil)

			p := w21aDecodeProblem(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w21aBadRequestType, p.Type)
			assert.Equal(t, w21aWrongTypeDetail, p.Detail)
			assert.Equal(t, []w21aParam{{Name: tc.wantField, Reason: "has the wrong type"}}, p.InvalidParams)
			assert.NotContains(t, w.Body.String(), "unmarshal", "the decoder's own text stays out of the answer")
			assert.NotContains(t, w.Body.String(), "Go struct", "the decoder's own text stays out of the answer")
			assert.Empty(t, starter.calls, "a refused request must not reach the service")
		})
	}
}
