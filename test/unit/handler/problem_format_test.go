package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

const (
	stubTraceID   = "4bf92f3577b34da6a3ce929d0e0e4736"
	stubRequestID = "b1a2c3d4-e5f6-7890-abcd-1234567890ef"
)

// instanceRouterWithCorrelationHeaders stands in for the library's correlation
// middleware, which sets the two response headers before any handler runs; the
// compat suite covers the real chain.
func instanceRouterWithCorrelationHeaders(h *handler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Header("X-Trace-ID", stubTraceID)
		c.Header("X-Request-ID", stubRequestID)
	})
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test"}) {
		r.Use(mw)
	}
	r.POST("/api/v1/instances", h.StartInstance)
	return r
}

func TestProblem_CarriesTheCorrelationIds(t *testing.T) {
	router := instanceRouterWithCorrelationHeaders(newInstanceHandler(&fakeInstanceService{}))

	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{}))

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var prob map[string]any
	decodeJSON(t, w.Body, &prob)
	assert.Equal(t, stubTraceID, prob["trace_id"])
	assert.Equal(t, stubRequestID, prob["request_id"])
}

func TestStrictBodies_AnUnknownFieldIsRefusedNamed(t *testing.T) {
	called := false
	router := newRouter(newInstanceHandler(&fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			called = true
			return nil, nil
		},
	}))

	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": uuid.New(), "unknown_member": "typo",
	}))

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, called, "a refused body must not reach the service")
	var prob struct {
		Code          string `json:"code"`
		InvalidParams []struct {
			Name   string `json:"name"`
			Reason string `json:"reason"`
		} `json:"invalid_params"`
	}
	decodeJSON(t, w.Body, &prob)
	assert.Equal(t, "BAD_REQUEST", prob.Code)
	require.Len(t, prob.InvalidParams, 1)
	assert.Equal(t, "unknown_member", prob.InvalidParams[0].Name)
	assert.Equal(t, "is not a known field", prob.InvalidParams[0].Reason)
}

func TestInternalBodies_ANewerSendersExtraFieldIsTolerated(t *testing.T) {
	t.Run("an event envelope", func(t *testing.T) {
		router := newInternalRouter(newEventsHandler(newEventsFakes()))
		body := envelope("SomeFutureEvent", uuid.New(), testTenantID, time.Now(), map[string]any{})
		body["added_by_a_newer_producer"] = "x"

		w := postEvent(router, body)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("a connector task callback", func(t *testing.T) {
		completed := false
		router := newInternalRouter(newConnectorTaskHandler(&fakeConnectorTaskService{
			complete: func(context.Context, uuid.UUID, uuid.UUID, map[string]any) error {
				completed = true
				return nil
			},
		}))

		w := do(router, internalReq(http.MethodPost, "/api/v1/internal/connector-tasks/"+uuid.NewString()+"/complete", map[string]any{
			"tenant_id": testTenantID, "output": map[string]any{}, "added_by_a_newer_worker": true,
		}))

		assert.Equal(t, http.StatusNoContent, w.Code)
		assert.True(t, completed)
	})
}

type fakeUserTaskPauser struct{ called bool }

func (f *fakeUserTaskPauser) PauseUserTasks(context.Context, uuid.UUID, uuid.UUID) error {
	f.called = true
	return nil
}

func TestInternalBodies_EveryInternalEndpointToleratesAnAddedField(t *testing.T) {
	userID := uuid.NewString()
	cases := []struct {
		name, path string
		body       map[string]any
		want       int
	}{
		{"reassign delegate", "/api/v1/internal/workflows/reassign-delegate", map[string]any{
			"tenant_id": testTenantID, "old_delegate_id": uuid.New(), "new_delegate_id": uuid.New(), "added_by_a_newer_caller": 1,
		}, http.StatusOK},
		{"cancel by delegate", "/api/v1/internal/workflows/cancel-by-delegate", map[string]any{
			"tenant_id": testTenantID, "delegate_user_id": uuid.New(), "added_by_a_newer_caller": 1,
		}, http.StatusOK},
		{"pause user tasks", "/api/v1/internal/users/" + userID + "/pause-tasks", map[string]any{
			"tenant_id": testTenantID, "added_by_a_newer_caller": 1,
		}, http.StatusNoContent},
	}
	gin.SetMode(gin.TestMode)
	pauser := &fakeUserTaskPauser{}
	h := handler.New(handler.Services{WorkflowClient: &fakeWorkflowClient{}, UserTaskPauser: pauser})
	r := gin.New()
	r.POST("/api/v1/internal/workflows/reassign-delegate", h.ReassignDelegate)
	r.POST("/api/v1/internal/workflows/cancel-by-delegate", h.CancelByDelegate)
	r.POST("/api/v1/internal/users/:user_id/pause-tasks", h.PauseUserTasks)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := do(r, internalReq(http.MethodPost, tc.path, tc.body))
			assert.Equal(t, tc.want, w.Code, w.Body.String())
		})
	}
	assert.True(t, pauser.called)
}

func TestStrictBodies_TrailingDataAfterTheValueIsRefused(t *testing.T) {
	called := false
	router := newRouter(newInstanceHandler(&fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			called = true
			return &port.Instance{}, nil
		},
	}))
	r := httptest.NewRequest(http.MethodPost, instancesPath(),
		strings.NewReader(`{"subject_id":"SUBJ-1","business_key":"TND-001","workflow_version_id":"`+uuid.NewString()+`"}{"zzz":1}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())

	w := do(router, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, called, "a body with a second value must not reach the service")
}

func TestStrictBodies_EveryPublicEndpointWithABodyRefusesAnUnknownField(t *testing.T) {
	id := uuid.NewString()
	paths := map[string]string{
		"start instance": "/api/v1/instances",
		"pause":          "/api/v1/instances/" + id + "/pause",
		"resume":         "/api/v1/instances/" + id + "/resume",
		"cancel":         "/api/v1/instances/" + id + "/cancel",
		"terminate":      "/api/v1/instances/" + id + "/terminate",
		"force forward":  "/api/v1/instances/" + id + "/force-forward",
		"force back":     "/api/v1/instances/" + id + "/force-back",
		"node override":  "/api/v1/instances/" + id + "/nodes/review/override",
		"outside answer": "/api/v1/instances/" + id + "/nodes/review/complete",
		"boundary fire":  "/api/v1/instances/" + id + "/nodes/review/boundaries/timeout/fire",
		"claim task":     "/api/v1/tasks/" + id + "/claim",
		"complete task":  "/api/v1/tasks/" + id + "/complete",
		"defer task":     "/api/v1/tasks/" + id + "/defer",
		"reassign task":  "/api/v1/tasks/" + id + "/reassign",
	}
	called := false
	router := newRouter(handler.New(handler.Services{
		Instances: &fakeInstanceService{start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			called = true
			return &port.Instance{}, nil
		}},
		Tasks: &fakeTaskService{},
	}))
	for name, path := range paths {
		t.Run(name, func(t *testing.T) {
			r := req(http.MethodPost, path, map[string]any{"unknown_member": "typo"})
			r.Header.Set("x-tenant-roles", "tenant_admin")
			w := do(router, r)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			var prob struct {
				InvalidParams []struct {
					Name   string `json:"name"`
					Reason string `json:"reason"`
				} `json:"invalid_params"`
			}
			decodeJSON(t, w.Body, &prob)
			require.Len(t, prob.InvalidParams, 1, "the refusal must name the unknown field, not fail on something else")
			assert.Equal(t, "unknown_member", prob.InvalidParams[0].Name)
		})
	}
	assert.False(t, called)
}
