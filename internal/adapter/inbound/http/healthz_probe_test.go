package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthz_WithRequestID_EchoesItAndIsCountedAndLoggedUnderItsRoute(t *testing.T) {
	rig := hrfNewRig(t, "test")
	before := hrfRequestsTotal(t, http.MethodGet, "/healthz", "2xx")

	w := rig.do(http.MethodGet, "/healthz", map[string]string{"X-Request-ID": "hrf-health-1"})

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", hrfBody(t, w)["status"])
	assert.Equal(t, "hrf-health-1", w.Header().Get("X-Request-ID"))
	traceID := w.Header().Get("X-Trace-ID")
	require.NotEmpty(t, traceID)
	assert.Equal(t, before+1, hrfRequestsTotal(t, http.MethodGet, "/healthz", "2xx"))
	entry := hrfAccessLogFor(t, rig.logger, "/healthz")
	assert.Equal(t, "info", entry.level)
	assert.Equal(t, "hrf-health-1", entry.fields["request_id"])
	assert.Equal(t, traceID, entry.fields["trace_id"])
}
