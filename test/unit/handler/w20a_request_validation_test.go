package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	w20aBadRequestType      = "https://errors.bcbp.io/execution/bad-request"
	w20aPayloadTooLargeType = "https://errors.bcbp.io/execution/payload-too-large"
	w20aInvalidFieldsDetail = "request body has missing or invalid fields"
	w20aNotJSONDetail       = "request body is not valid JSON"
	w20aTrailingDataDetail  = "request body has data after its JSON value"
)

type w20aParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type w20aProblem struct {
	Type          string      `json:"type"`
	Status        int         `json:"status"`
	Detail        string      `json:"detail"`
	Code          string      `json:"code"`
	Instance      string      `json:"instance"`
	InvalidParams []w20aParam `json:"invalid_params"`
}

type w20aCall struct {
	op       string
	tenantID uuid.UUID
	userID   uuid.UUID
	id       uuid.UUID
	version  int64
}

// w20aDetail holds the arguments that decide what a call does beyond the caller's ids.
type w20aDetail struct {
	waitKey   string
	variables map[string]any
	nodeKey   string
	newUserID uuid.UUID
}

// w20aValidationRig puts recording fakes behind the public route table; every service call lands in calls.
type w20aValidationRig struct {
	calls   []w20aCall
	details []w20aDetail
	router  http.Handler
}

func w20aNewValidationRig() *w20aValidationRig {
	rig := &w20aValidationRig{}
	instances := &fakeInstanceService{
		start: func(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
			rig.calls = append(rig.calls, w20aCall{op: "start", tenantID: in.TenantID, userID: in.StartedByUserID, id: in.WorkflowVersionID})
			return &port.Instance{ID: testInstID}, nil
		},
		pause: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, _ string, recordVersion int64) error {
			rig.calls = append(rig.calls, w20aCall{"pause", tenantID, actorUserID, instanceID, recordVersion})
			return nil
		},
		cancel: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, _ string, recordVersion int64) error {
			rig.calls = append(rig.calls, w20aCall{"cancel", tenantID, actorUserID, instanceID, recordVersion})
			return nil
		},
		list: func(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, _ port.InstanceFilter, _ port.Page) (port.PageResult[*port.Instance], error) {
			rig.calls = append(rig.calls, w20aCall{op: "list", tenantID: tenantID, userID: scope.CallerUserID})
			return port.PageResult[*port.Instance]{}, nil
		},
		answerOutsideWait: func(_ context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, key string, variables map[string]any) error {
			rig.details = append(rig.details, w20aDetail{waitKey: key, variables: variables})
			rig.calls = append(rig.calls, w20aCall{op: "answer", tenantID: tenantID, userID: scope.CallerUserID, id: instanceID})
			return nil
		},
	}
	tasks := &fakeTaskService{
		claim: func(_ context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
			rig.calls = append(rig.calls, w20aCall{"claim", tenantID, userID, taskID, recordVersion})
			return newTestTask(), nil
		},
		complete: func(_ context.Context, tenantID, taskID, userID uuid.UUID, _ json.RawMessage, recordVersion int64) (*port.Task, error) {
			rig.calls = append(rig.calls, w20aCall{"complete", tenantID, userID, taskID, recordVersion})
			return newTestTask(), nil
		},
		deferTask: func(_ context.Context, tenantID, taskID, userID uuid.UUID, _ string, recordVersion int64) (*port.Task, error) {
			rig.calls = append(rig.calls, w20aCall{"defer", tenantID, userID, taskID, recordVersion})
			return newTestTask(), nil
		},
		overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			rig.details = append(rig.details, w20aDetail{nodeKey: in.NodeKey, newUserID: in.NewUserID})
			rig.calls = append(rig.calls, w20aCall{"override", in.TenantID, in.ActorUserID, in.InstanceID, in.RecordVersion})
			return &port.AssigneeOverride{}, nil
		},
	}
	rig.router = newRouter(handler.New(handler.Services{Instances: instances, Tasks: tasks}))
	return rig
}

func (r *w20aValidationRig) send(method, target, body string, admin bool) *httptest.ResponseRecorder {
	hr := httptest.NewRequest(method, target, strings.NewReader(body))
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("x-tenant-id", testTenantID.String())
	hr.Header.Set("x-user-id", testUserID.String())
	if admin {
		hr.Header.Set("x-tenant-roles", "tenant_admin")
	}
	w := httptest.NewRecorder()
	r.router.ServeHTTP(w, hr)
	return w
}

func w20aDecode(t *testing.T, w *httptest.ResponseRecorder) w20aProblem {
	t.Helper()
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var p w20aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, w.Code, p.Status)
	return p
}

type w20aRoute struct {
	method   string
	path     string
	admin    bool
	okBody   string
	okStatus int
	okCall   w20aCall
	// okDetails is nil for routes whose service call carries nothing beyond the ids.
	okDetails []w20aDetail
}

func w20aRoutes(versionID, newUserID uuid.UUID) map[string]w20aRoute {
	inst := "/api/v1/instances/" + testInstID.String()
	task := "/api/v1/tasks/" + testTaskID.String()
	return map[string]w20aRoute{
		"start": {
			method: http.MethodPost, path: "/api/v1/instances", okStatus: http.StatusCreated,
			okBody: `{"subject_id":"subj-1","business_key":"bk-1","workflow_version_id":"` + versionID.String() + `"}`,
			okCall: w20aCall{"start", testTenantID, testUserID, versionID, 0},
		},
		"pause": {
			method: http.MethodPost, path: inst + "/pause", admin: true, okStatus: http.StatusAccepted,
			okBody: `{"record_version":3}`, okCall: w20aCall{"pause", testTenantID, testUserID, testInstID, 3},
		},
		"cancel": {
			method: http.MethodPost, path: inst + "/cancel", admin: true, okStatus: http.StatusAccepted,
			okBody: `{"reason":"stuck","record_version":3}`, okCall: w20aCall{"cancel", testTenantID, testUserID, testInstID, 3},
		},
		"claim": {
			method: http.MethodPost, path: task + "/claim", okStatus: http.StatusAccepted,
			okBody: `{"record_version":3}`, okCall: w20aCall{"claim", testTenantID, testUserID, testTaskID, 3},
		},
		"complete task": {
			method: http.MethodPost, path: task + "/complete", okStatus: http.StatusAccepted,
			okBody: `{"result_json":{"decision":"approve"},"record_version":3}`, okCall: w20aCall{"complete", testTenantID, testUserID, testTaskID, 3},
		},
		"defer": {
			method: http.MethodPost, path: task + "/defer", okStatus: http.StatusOK,
			okBody: `{"reason":"later","record_version":3}`, okCall: w20aCall{"defer", testTenantID, testUserID, testTaskID, 3},
		},
		"override": {
			method: http.MethodPost, path: inst + "/nodes/finance%2FReview/override", admin: true, okStatus: http.StatusOK,
			okBody: `{"new_user_id":"` + newUserID.String() + `","record_version":3}`, okCall: w20aCall{"override", testTenantID, testUserID, testInstID, 3},
			okDetails: []w20aDetail{{nodeKey: "finance/Review", newUserID: newUserID}},
		},
		"complete outside wait": {
			method: http.MethodPost, path: inst + "/nodes/Wait_1/complete", okStatus: http.StatusAccepted,
			okBody: `{"variables":{"answer":"yes"}}`, okCall: w20aCall{op: "answer", tenantID: testTenantID, userID: testUserID, id: testInstID},
			okDetails: []w20aDetail{{waitKey: "Wait_1", variables: map[string]any{"answer": "yes"}}},
		},
		"fire boundary": {
			method: http.MethodPost, path: inst + "/nodes/Wait_1/boundaries/Timer_1/fire", okStatus: http.StatusAccepted,
			okBody: `{"variables":{"answer":"yes"}}`, okCall: w20aCall{op: "answer", tenantID: testTenantID, userID: testUserID, id: testInstID},
			okDetails: []w20aDetail{{waitKey: dsl.BoundaryWaitKey("Wait_1", "Timer_1"), variables: map[string]any{"answer": "yes"}}},
		},
	}
}

// Each route accepts its documented body: the refusals below are caused by the field under test.
func TestW20aPublicRoutes_ValidBody_ReachesTheServiceWithTheCallersIds(t *testing.T) {
	versionID, newUserID := uuid.New(), uuid.New()
	for name, route := range w20aRoutes(versionID, newUserID) {
		t.Run(name, func(t *testing.T) {
			rig := w20aNewValidationRig()

			w := rig.send(route.method, route.path, route.okBody, route.admin)

			require.Equal(t, route.okStatus, w.Code, w.Body.String())
			require.Equal(t, []w20aCall{route.okCall}, rig.calls)
			require.Equal(t, route.okDetails, rig.details)
		})
	}
}

func TestW20aPublicRoutes_MissingOrBlankFields_AreRefusedWithTheFieldNamed(t *testing.T) {
	versionID, newUserID := uuid.New(), uuid.New()
	nilUUID := uuid.Nil.String()
	required := func(names ...string) []w20aParam {
		params := make([]w20aParam, len(names))
		for i, name := range names {
			params[i] = w20aParam{Name: name, Reason: "is required"}
		}
		return params
	}
	startBody := func(subject, key, version string) string {
		return `{"subject_id":` + subject + `,"business_key":` + key + `,"workflow_version_id":` + version + `}`
	}
	validVersion := `"` + versionID.String() + `"`
	cases := []struct {
		name       string
		route      string
		body       string
		wantParams []w20aParam
	}{
		{"start without subject_id", "start", `{"business_key":"bk-1","workflow_version_id":` + validVersion + `}`, required("subject_id")},
		{"start with an empty subject_id", "start", startBody(`""`, `"bk-1"`, validVersion), required("subject_id")},
		{"start without business_key", "start", `{"subject_id":"subj-1","workflow_version_id":` + validVersion + `}`, required("business_key")},
		{"start with an empty business_key", "start", startBody(`"subj-1"`, `""`, validVersion), required("business_key")},
		{"start without workflow_version_id", "start", `{"subject_id":"subj-1","business_key":"bk-1"}`, required("workflow_version_id")},
		{"start with the nil workflow_version_id", "start", startBody(`"subj-1"`, `"bk-1"`, `"`+nilUUID+`"`), required("workflow_version_id")},
		{"pause without record_version", "pause", `{}`, required("record_version")},
		{"pause with record_version 0", "pause", `{"record_version":0}`, required("record_version")},
		{"cancel without reason", "cancel", `{"record_version":3}`, required("reason")},
		{"cancel with an empty reason", "cancel", `{"reason":"","record_version":3}`, required("reason")},
		{"cancel without record_version", "cancel", `{"reason":"stuck"}`, required("record_version")},
		{"cancel without reason and record_version", "cancel", `{}`, required("reason", "record_version")},
		{"claim without record_version", "claim", `{}`, required("record_version")},
		{"complete task without record_version", "complete task", `{"result_json":{"decision":"approve"}}`, required("record_version")},
		{"defer without record_version", "defer", `{"reason":"later"}`, required("record_version")},
		{"override without new_user_id", "override", `{"record_version":3}`, required("new_user_id")},
		{"override with the nil new_user_id", "override", `{"new_user_id":"` + nilUUID + `","record_version":3}`, required("new_user_id")},
	}
	routes := w20aRoutes(versionID, newUserID)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			route := routes[tc.route]
			rig := w20aNewValidationRig()

			w := rig.send(route.method, route.path, tc.body, route.admin)

			p := w20aDecode(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w20aBadRequestType, p.Type)
			assert.Equal(t, w20aInvalidFieldsDetail, p.Detail)
			assert.Equal(t, tc.wantParams, p.InvalidParams)
			assert.Empty(t, rig.calls, "a refused request must not reach the service")
		})
	}
}

func TestW20aPublicRoutes_UnreadableBody_IsRefusedAsNotJSON(t *testing.T) {
	versionID, newUserID := uuid.New(), uuid.New()
	cases := []struct {
		name       string
		route      string
		body       string
		wantDetail string
	}{
		{"pause with a truncated body", "pause", `{"record_version":`, w20aNotJSONDetail},
		{"pause with an empty body", "pause", ``, w20aNotJSONDetail},
		{"pause with a whitespace-only body", "pause", " \n ", w20aNotJSONDetail},
		{"override with a truncated body", "override", `{"new_user_id":`, w20aNotJSONDetail},
		{"override with an empty body", "override", ``, w20aNotJSONDetail},
		{"complete outside wait with plain text", "complete outside wait", `not json at all`, w20aNotJSONDetail},
		{"fire with a truncated body", "fire boundary", `{"variables":`, w20aNotJSONDetail},
		{"fire with plain text", "fire boundary", `not json at all`, w20aNotJSONDetail},
		{"fire with a second JSON value", "fire boundary", `{"variables":{}} {"variables":{}}`, w20aTrailingDataDetail},
	}
	routes := w20aRoutes(versionID, newUserID)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			route := routes[tc.route]
			rig := w20aNewValidationRig()

			w := rig.send(route.method, route.path, tc.body, route.admin)

			p := w20aDecode(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w20aBadRequestType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Empty(t, p.InvalidParams)
			assert.Empty(t, rig.calls, "a refused request must not reach the service")
		})
	}
}

func TestW20aStartInstance_BodyOverTheCap_IsRefusedWith413(t *testing.T) {
	rig := w20aNewValidationRig()
	const overTenMiB = 10<<20 + 1
	body := `{"subject_id":"` + strings.Repeat("a", overTenMiB) + `"}`

	w := rig.send(http.MethodPost, "/api/v1/instances", body, false)

	p := w20aDecode(t, w)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	assert.Equal(t, "PAYLOAD_TOO_LARGE", p.Code)
	assert.Equal(t, w20aPayloadTooLargeType, p.Type)
	assert.Equal(t, "request body exceeds the 10 MB limit", p.Detail)
	assert.Equal(t, "/api/v1/instances", p.Instance)
	assert.Empty(t, rig.calls, "an oversized request must not reach the service")
}

func TestW20aListInstances_WorkflowVersionIDNotAUUID_IsRefusedWithTheQueryNamed(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{"free text", "not-a-uuid"},
		{"one character short of a uuid", "11111111-1111-1111-1111-11111111111"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w20aNewValidationRig()

			w := rig.send(http.MethodGet, "/api/v1/instances?workflow_version_id="+tc.query, "", false)

			p := w20aDecode(t, w)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w20aBadRequestType, p.Type)
			assert.Equal(t, "invalid workflow_version_id query parameter", p.Detail)
			assert.Empty(t, rig.calls, "a refused request must not reach the service")
		})
	}
}
