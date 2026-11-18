package handler_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW15bGuardFailed = errors.New("w15b: counting live instances failed: password=hunter2")

type w15bGuardCall struct{ tenantID, workflowID uuid.UUID }

// w15bArchiveGuard answers with a fixed count and error and records who asked.
type w15bArchiveGuard struct {
	count int32
	err   error
	calls []w15bGuardCall
}

func (f *w15bArchiveGuard) CheckActiveInstances(_ context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error) {
	f.calls = append(f.calls, w15bGuardCall{tenantID, workflowID})
	return f.count > 0, f.count, f.err
}

var _ port.ArchiveGuard = (*w15bArchiveGuard)(nil)

type w15bPauseCall struct {
	tenantID, userID uuid.UUID
	departmentID     *uuid.UUID
}

type w15bUserTaskPauser struct{ calls []w15bPauseCall }

func (f *w15bUserTaskPauser) PauseUserTasks(_ context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error {
	f.calls = append(f.calls, w15bPauseCall{tenantID, userID, departmentID})
	return nil
}

var _ port.UserTaskPauser = (*w15bUserTaskPauser)(nil)

func w15bActivePath(workflowID string) string {
	return "/api/v1/internal/workflows/" + workflowID + "/active-instances"
}

func TestActiveInstances_NoLiveInstances_Returns200WithAnEmptyCount(t *testing.T) {
	t.Parallel()

	tenantID, workflowID := uuid.New(), uuid.New()
	guard := &w15bArchiveGuard{}
	router := w15bInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))

	w := w15bServe(router, http.MethodGet, w15bActivePath(workflowID.String())+"?tenant_id="+tenantID.String(), "", "")

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.JSONEq(t, `{"has_active":false,"count":0}`, w.Body.String())
	assert.Equal(t, []w15bGuardCall{{tenantID, workflowID}}, guard.calls)
}

func TestActiveInstances_RefusedRequest_Returns400NamingTheParameter(t *testing.T) {
	t.Parallel()

	validTenant, validWorkflow := uuid.New().String(), uuid.New().String()
	cases := []struct {
		name       string
		workflowID string
		query      string
		wantDetail string
		wantParam  string
		wantReason string
	}{
		{"workflow_id not a UUID", "abc", "tenant_id=" + validTenant, "invalid workflow_id path parameter", "workflow_id", "is not valid"},
		{"workflow_id all zero", uuid.Nil.String(), "tenant_id=" + validTenant, "invalid workflow_id path parameter", "workflow_id", "is not valid"},
		{"tenant_id absent", validWorkflow, "", "tenant_id query parameter is required", "tenant_id", "is required"},
		{"tenant_id empty", validWorkflow, "tenant_id=", "invalid tenant_id query parameter", "tenant_id", "is not valid"},
		{"tenant_id all zero", validWorkflow, "tenant_id=" + uuid.Nil.String(), "invalid tenant_id query parameter", "tenant_id", "is not valid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			guard := &w15bArchiveGuard{count: 3}
			router := w15bInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))
			target := w15bActivePath(tc.workflowID)
			if tc.query != "" {
				target += "?" + tc.query
			}

			w := w15bServe(router, http.MethodGet, target, "", "")

			p := w15bDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w15bBadRequestType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, w15bInvalidParam{Name: tc.wantParam, Reason: tc.wantReason}, p.InvalidParams[0])
			assert.Empty(t, guard.calls, "a refused request must not reach the guard")
		})
	}
}

func TestActiveInstances_GuardFailure_Returns500WithTheGenericInternalProblem(t *testing.T) {
	t.Parallel()

	tenantID, workflowID := uuid.New(), uuid.New()
	guard := &w15bArchiveGuard{err: errW15bGuardFailed}
	router := w15bInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))

	w := w15bServe(router, http.MethodGet, w15bActivePath(workflowID.String())+"?tenant_id="+tenantID.String(), "", "")

	p := w15bDecodeProblem(t, w, http.StatusInternalServerError)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/internal-error", p.Type)
	assert.Equal(t, "an unexpected error occurred", p.Detail)
	assert.NotContains(t, w.Body.String(), "hunter2", "the failure text is not echoed")
	assert.Equal(t, []w15bGuardCall{{tenantID, workflowID}}, guard.calls)
}

func TestPauseUserTasks_RefusedRequest_ReturnsTheProblemAndPausesNothing(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	validBody := `{"tenant_id":"` + tenantID.String() + `"}`
	oversized := `{"tenant_id":"` + tenantID.String() + `","padding":"` + strings.Repeat("a", 11<<20) + `"}`
	cases := []struct {
		name        string
		userID      string
		contentType string
		body        string
		wantStatus  int
		wantCode    string
		wantType    string
		wantDetail  string
		wantParams  []w15bInvalidParam
	}{
		{
			name: "text/plain content type", userID: uuid.NewString(), contentType: "text/plain", body: validBody,
			wantStatus: http.StatusUnsupportedMediaType, wantCode: "UNSUPPORTED_MEDIA_TYPE",
			wantType:   "https://errors.bcbp.io/execution/unsupported-media-type",
			wantDetail: "request body must be application/json",
		},
		{
			name: "empty body", userID: uuid.NewString(), contentType: "application/json", body: "",
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantType: w15bBadRequestType,
			wantDetail: "request body is not valid JSON",
		},
		{
			name: "body over the 10 MB limit", userID: uuid.NewString(), contentType: "application/json", body: oversized,
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: "PAYLOAD_TOO_LARGE",
			wantType:   "https://errors.bcbp.io/execution/payload-too-large",
			wantDetail: "request body exceeds the 10 MB limit",
		},
		{
			name: "user_id not a UUID", userID: "nope", contentType: "application/json", body: validBody,
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantType: w15bBadRequestType,
			wantDetail: "invalid user_id path parameter",
			wantParams: []w15bInvalidParam{{Name: "user_id", Reason: "is not valid"}},
		},
		{
			name: "user_id not a UUID with a body that is also refused", userID: "nope", contentType: "application/json", body: "{",
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantType: w15bBadRequestType,
			wantDetail: "invalid user_id path parameter",
			wantParams: []w15bInvalidParam{{Name: "user_id", Reason: "is not valid"}},
		},
		{
			name: "tenant_id missing from the body", userID: uuid.NewString(), contentType: "application/json", body: `{}`,
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantType: w15bBadRequestType,
			wantDetail: "request body has missing or invalid fields",
			wantParams: []w15bInvalidParam{{Name: "tenant_id", Reason: "is required"}},
		},
		{
			name: "tenant_id key in the wrong case", userID: uuid.NewString(), contentType: "application/json",
			body:       `{"Tenant_ID":"` + tenantID.String() + `"}`,
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantType: w15bBadRequestType,
			wantDetail: "request body has fields this endpoint does not define",
			wantParams: []w15bInvalidParam{{Name: "Tenant_ID", Reason: "is not a known field"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pauser := &w15bUserTaskPauser{}
			router := w15bInternalRouter(handler.New(handler.Services{UserTaskPauser: pauser}))

			w := w15bServe(router, http.MethodPost, "/api/v1/internal/users/"+url.PathEscape(tc.userID)+"/pause-tasks", tc.contentType, tc.body)

			p := w15bDecodeProblem(t, w, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Equal(t, tc.wantParams, p.InvalidParams)
			assert.Empty(t, pauser.calls, "a refused request must not pause any task")
		})
	}
}
