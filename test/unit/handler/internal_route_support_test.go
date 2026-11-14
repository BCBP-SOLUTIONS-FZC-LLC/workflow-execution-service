package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// hhaInternalRouter registers every /api/v1/internal route this package's
// handler tests need, with no gateway middleware: system callers carry no
// user identity.
func hhaInternalRouter(h *handler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	rg := r.Group("/api/v1/internal")
	rg.POST("/workflows/reassign-delegate", h.ReassignDelegate)
	rg.POST("/workflows/cancel-by-delegate", h.CancelByDelegate)
	rg.GET("/workflows/delegate-impact", h.DelegateImpact)
	rg.GET("/workflows/:workflow_id/active-instances", h.ActiveInstances)
	rg.POST("/users/:user_id/pause-tasks", h.PauseUserTasks)
	rg.POST("/events/delegation", h.HandleDelegationEvents)
	rg.POST("/events/user-profile", h.HandleUserProfileEvents)
	rg.POST("/connector-tasks/:id/complete", h.CompleteConnectorTask)
	rg.POST("/connector-tasks/:id/fail", h.FailConnectorTask)
	return r
}

func hhaServe(router *gin.Engine, method, target, contentType, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func hhaPostJSON(router *gin.Engine, target, body string) *httptest.ResponseRecorder {
	return hhaServe(router, http.MethodPost, target, "application/json", body)
}

type hhaInvalidParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type hhaProblem struct {
	Status        int               `json:"status"`
	Code          string            `json:"code"`
	Detail        string            `json:"detail"`
	InvalidParams []hhaInvalidParam `json:"invalid_params"`
}

// hhaDecodeProblem fails the test unless the response is an RFC 7807 body
// with the given status, and returns it decoded.
func hhaDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) hhaProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p hhaProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

// hhaAssertNoInternalText checks that decoder and validator wording did not
// reach the caller.
func hhaAssertNoInternalText(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	body := w.Body.String()
	for _, leak := range []string{"cannot unmarshal", "Go struct", "Go value", "uuid.UUID", "map[string]", "invalid UUID", "Key: '", "Error:Field validation", "json:"} {
		assert.NotContains(t, body, leak)
	}
}

func hhaJSONString(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

type hhaWorkflowClient struct {
	reassignCalls []port.ReassignDelegateInput
	cancelCalls   []port.CancelByDelegateInput
	impactCalls   []port.DelegateImpactInput
	impactResult  port.DelegateImpactResult
}

func (f *hhaWorkflowClient) ReassignDelegate(_ context.Context, in port.ReassignDelegateInput) (int, error) {
	f.reassignCalls = append(f.reassignCalls, in)
	return 0, nil
}

func (f *hhaWorkflowClient) CancelByDelegate(_ context.Context, in port.CancelByDelegateInput) (int, error) {
	f.cancelCalls = append(f.cancelCalls, in)
	return 0, nil
}

func (f *hhaWorkflowClient) DelegateImpact(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	f.impactCalls = append(f.impactCalls, in)
	return f.impactResult, nil
}

var _ port.WorkflowClient = (*hhaWorkflowClient)(nil)

type hhaCompleteCall struct {
	tenantID, taskID uuid.UUID
	output           map[string]any
}

type hhaFailCall struct {
	tenantID, taskID uuid.UUID
	errorClass       string
	output           map[string]any
}

type hhaConnectorTasks struct {
	completeCalls []hhaCompleteCall
	failCalls     []hhaFailCall
}

func (f *hhaConnectorTasks) Complete(_ context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	f.completeCalls = append(f.completeCalls, hhaCompleteCall{tenantID, taskID, output})
	return nil
}

func (f *hhaConnectorTasks) Fail(_ context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error {
	f.failCalls = append(f.failCalls, hhaFailCall{tenantID, taskID, errorClass, output})
	return nil
}

var _ port.ConnectorTaskService = (*hhaConnectorTasks)(nil)

type hhaTenantWorkflow struct{ tenantID, workflowID uuid.UUID }

// hhaArchiveGuard answers like a store: only the registered
// (tenant, workflow) pairs have active instances, every other pair has none.
type hhaArchiveGuard struct {
	active map[hhaTenantWorkflow]int32
	calls  []hhaTenantWorkflow
}

func (f *hhaArchiveGuard) CheckActiveInstances(_ context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error) {
	key := hhaTenantWorkflow{tenantID, workflowID}
	f.calls = append(f.calls, key)
	count := f.active[key]
	return count > 0, count, nil
}

var _ port.ArchiveGuard = (*hhaArchiveGuard)(nil)

type hhaPauseCall struct {
	tenantID, userID uuid.UUID
	departmentID     *uuid.UUID
}

type hhaUserTaskPauser struct{ calls []hhaPauseCall }

func (f *hhaUserTaskPauser) PauseUserTasks(_ context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error {
	f.calls = append(f.calls, hhaPauseCall{tenantID, userID, departmentID})
	return nil
}

var _ port.UserTaskPauser = (*hhaUserTaskPauser)(nil)

func hhaDecodeInto(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), v), w.Body.String())
}
