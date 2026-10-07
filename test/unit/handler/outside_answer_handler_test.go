package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func completePath(node string) string {
	return instancePath(testInstID) + "/nodes/" + node + "/complete"
}

func firePath(node, boundary string) string {
	return instancePath(testInstID) + "/nodes/" + node + "/boundaries/" + boundary + "/fire"
}

type answered struct {
	key       string
	variables map[string]any
	calls     int
}

func recordingInstances(t *testing.T, a *answered, err error) *fakeInstanceService {
	return &fakeInstanceService{answerOutsideWait: func(_ context.Context, tenantID, instanceID, _ uuid.UUID, key string, variables map[string]any) error {
		a.calls++
		a.key, a.variables = key, variables
		assert.Equal(t, testTenantID, tenantID)
		assert.Equal(t, testInstID, instanceID)
		return err
	}}
}

// TestCompleteOutsideWait pins that a receive task is addressed by its node
// key, percent-encoded in the path, and its answer's variables are passed on.
func TestCompleteOutsideWait(t *testing.T) {
	var got answered
	router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

	w := do(router, asAdmin(req(http.MethodPost, completePath("finance%2FReceive_Bid"), map[string]any{"variables": map[string]any{"bid": "42"}})))

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, "finance/Receive_Bid", got.key)
	assert.Equal(t, map[string]any{"bid": "42"}, got.variables)
}

func TestCompleteOutsideWait_NeedsNoBody(t *testing.T) {
	var got answered
	router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

	w := do(router, asAdmin(req(http.MethodPost, completePath("finance%2FReceive_Bid"), nil)))

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Empty(t, got.variables)
}

// TestFireOutsideBoundary pins that a boundary is addressed by its host and
// its own id, as dsl.BoundaryWaitKey joins them.
func TestFireOutsideBoundary(t *testing.T) {
	var got answered
	router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

	w := do(router, asAdmin(req(http.MethodPost, firePath("finance%2FPrepare", "Boundary_Withdraw"), nil)))

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, "finance/Prepare#Boundary_Withdraw", got.key)
}

func TestOutsideAnswer_NotAdmin_Forbidden(t *testing.T) {
	var got answered
	router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

	w := do(router, req(http.MethodPost, completePath("finance%2FReceive_Bid"), nil))

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Zero(t, got.calls)
}

func TestOutsideAnswer_Errors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code int
		body string
	}{
		{"not an outside wait", port.ErrWaitNotFound, http.StatusNotFound, "WAIT_NOT_FOUND"},
		{"not waiting", port.ErrNotWaiting, http.StatusConflict, "NOT_WAITING"},
		{"no such instance", port.ErrInstanceNotFound, http.StatusNotFound, "INSTANCE_NOT_FOUND"},
		{"instance state", port.ErrInvalidInstanceState, http.StatusConflict, "INVALID_INSTANCE_STATE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got answered
			router := newRouter(newInstanceHandler(recordingInstances(t, &got, tc.err)))

			w := do(router, asAdmin(req(http.MethodPost, completePath("finance%2FReceive_Bid"), nil)))

			require.Equal(t, tc.code, w.Code)
			var body problemBody
			decodeJSON(t, w.Body, &body)
			assert.Equal(t, tc.body, body.Code)
		})
	}
}
