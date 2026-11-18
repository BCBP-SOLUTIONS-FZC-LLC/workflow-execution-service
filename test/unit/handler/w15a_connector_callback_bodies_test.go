package handler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW15aConnectorStore = errors.New("w15a: connector task store is down")

type w15aCompleteCall struct {
	tenantID, taskID uuid.UUID
	output           map[string]any
}

type w15aFailCall struct {
	tenantID, taskID uuid.UUID
	errorClass       string
	output           map[string]any
}

// w15aConnectorTasks records every call it receives and answers failErr from Fail.
type w15aConnectorTasks struct {
	failErr       error
	completeCalls []w15aCompleteCall
	failCalls     []w15aFailCall
}

func (f *w15aConnectorTasks) Complete(_ context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	f.completeCalls = append(f.completeCalls, w15aCompleteCall{tenantID, taskID, output})
	return nil
}

func (f *w15aConnectorTasks) Fail(_ context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error {
	f.failCalls = append(f.failCalls, w15aFailCall{tenantID, taskID, errorClass, output})
	return f.failErr
}

var _ port.ConnectorTaskService = (*w15aConnectorTasks)(nil)

func w15aConnectorRouter(f *w15aConnectorTasks) http.Handler {
	return newInternalRouter(handler.New(handler.Services{ConnectorTasks: f}))
}

func w15aConnectorPath(taskID, outcome string) string {
	return "/api/v1/internal/connector-tasks/" + taskID + "/" + outcome
}

func TestCompleteConnectorTask_TenantIDMissingOrNil_Returns400NamingTheField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing", func(f map[string]any) { delete(f, "tenant_id") }},
		{"zero UUID", func(f map[string]any) { f["tenant_id"] = uuid.Nil.String() }},
		{"null", func(f map[string]any) { f["tenant_id"] = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &w15aConnectorTasks{}
			fields := map[string]any{"tenant_id": uuid.NewString(), "output": map[string]any{"k": "v"}}
			tc.mutate(fields)

			w := w15aPost(w15aConnectorRouter(f), w15aConnectorPath(uuid.NewString(), "complete"), w15aJSON(t, fields))

			p := w15aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, []w15aParam{{Name: "tenant_id", Reason: "is required"}}, p.InvalidParams)
			assert.Empty(t, f.completeCalls, "a refused body must not reach the connector task service")
		})
	}
}

func TestFailConnectorTask_ErrorClassMissingOrEmpty_Returns400NamingTheField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing", func(f map[string]any) { delete(f, "error_class") }},
		{"empty string", func(f map[string]any) { f["error_class"] = "" }},
		{"null", func(f map[string]any) { f["error_class"] = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &w15aConnectorTasks{}
			fields := map[string]any{"tenant_id": uuid.NewString(), "error_class": "upstream_error"}
			tc.mutate(fields)

			w := w15aPost(w15aConnectorRouter(f), w15aConnectorPath(uuid.NewString(), "fail"), w15aJSON(t, fields))

			p := w15aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, []w15aParam{{Name: "error_class", Reason: "is required"}}, p.InvalidParams)
			assert.Empty(t, f.failCalls, "a refused body must not reach the connector task service")
		})
	}
}

func TestFailConnectorTask_PathIDNotUUID_Returns400NamingTheID(t *testing.T) {
	for _, id := range []string{"not-a-uuid", "123", "00000000-0000-0000-0000-00000000000g"} {
		t.Run(id, func(t *testing.T) {
			f := &w15aConnectorTasks{}
			body := w15aJSON(t, map[string]any{"tenant_id": uuid.NewString(), "error_class": "upstream_error"})

			w := w15aPost(w15aConnectorRouter(f), w15aConnectorPath(id, "fail"), body)

			p := w15aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "invalid id path parameter", p.Detail)
			assert.Equal(t, []w15aParam{{Name: "id", Reason: "is not valid"}}, p.InvalidParams)
			assert.Empty(t, f.failCalls, "a malformed id must not reach the connector task service")
		})
	}
}

func TestFailConnectorTask_ServiceOutcome_RendersEmptyBodyOrProblemDocument(t *testing.T) {
	cases := []struct {
		name       string
		failErr    error
		wantStatus int
		wantCode   string
	}{
		{"success has no body", nil, http.StatusNoContent, ""},
		{"unclassified failure is an internal error", errW15aConnectorStore, http.StatusInternalServerError, "INTERNAL_ERROR"},
		{"wrapped unknown task is not found", fmt.Errorf("fail connector task: %w", port.ErrTaskNotFound), http.StatusNotFound, "TASK_NOT_FOUND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant, taskID := uuid.New(), uuid.New()
			f := &w15aConnectorTasks{failErr: tc.failErr}
			body := w15aJSON(t, map[string]any{
				"tenant_id": tenant.String(), "error_class": "upstream_error", "output": map[string]any{"attempt": 3},
			})

			w := w15aPost(w15aConnectorRouter(f), w15aConnectorPath(taskID.String(), "fail"), body)

			require.Len(t, f.failCalls, 1)
			got := f.failCalls[0]
			assert.Equal(t, tenant, got.tenantID)
			assert.Equal(t, taskID, got.taskID)
			assert.Equal(t, "upstream_error", got.errorClass)
			assert.Equal(t, map[string]any{"attempt": float64(3)}, got.output)
			if tc.wantStatus == http.StatusNoContent {
				require.Equal(t, http.StatusNoContent, w.Code)
				assert.Empty(t, w.Body.String(), "a success writes no body")
				assert.NotEqual(t, "application/problem+json", w.Header().Get("Content-Type"))
				return
			}
			p := w15aDecodeProblem(t, w, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.NotContains(t, w.Body.String(), "connector task store is down", "the raw error must not reach the caller")
		})
	}
}
