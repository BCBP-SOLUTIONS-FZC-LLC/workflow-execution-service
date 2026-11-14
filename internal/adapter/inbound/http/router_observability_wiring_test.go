package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_EveryResponseKind_CarriesCorrelationHeadersAndEchoesThemInBodies(t *testing.T) {
	const supplied = "hrf-observe-1"
	cases := []struct {
		name            string
		method          string
		path            string
		headers         map[string]string
		wantCode        int
		hasBody         bool
		wantProblemCode string
	}{
		{name: "probe 200", method: http.MethodGet, path: "/healthz", wantCode: http.StatusOK},
		{name: "readiness 200", method: http.MethodGet, path: "/readyz", wantCode: http.StatusOK},
		{name: "public handler 400", method: http.MethodGet, path: "/api/v1/tasks/not-a-uuid", headers: hrfAuthHeaders(), wantCode: http.StatusBadRequest, hasBody: true},
		{name: "public guard 401", method: http.MethodGet, path: "/api/v1/tasks/not-a-uuid", wantCode: http.StatusUnauthorized, hasBody: true},
		{name: "internal guard 401", method: http.MethodPost, path: "/api/v1/internal/events/tenant", wantCode: http.StatusUnauthorized, hasBody: true},
		{name: "no route 404", method: http.MethodGet, path: "/no/such/path", wantCode: http.StatusNotFound, hasBody: true, wantProblemCode: "NOT_FOUND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := hrfNewRig(t, "test")
			headers := map[string]string{"X-Request-ID": supplied}
			for k, v := range tc.headers {
				headers[k] = v
			}

			w := rig.do(tc.method, tc.path, headers)

			require.Equal(t, tc.wantCode, w.Code)
			assert.Equal(t, supplied, w.Header().Get("X-Request-ID"))
			traceID := w.Header().Get("X-Trace-ID")
			require.NotEmpty(t, traceID)
			if tc.hasBody {
				body := hrfBody(t, w)
				if tc.wantProblemCode != "" {
					assert.Equal(t, tc.wantProblemCode, body["code"])
				}
				assert.Equal(t, supplied, body["request_id"])
				assert.Equal(t, traceID, body["trace_id"])
			}
		})
	}
}

func TestRouter_DevAsyncAPIRoute_RunsOutsideTheObservabilityMiddlewares(t *testing.T) {
	rig := hrfNewRig(t, "dev")
	t.Chdir(t.TempDir())

	w := rig.do(http.MethodGet, "/asyncapi", map[string]string{"X-Request-ID": "hrf-observe-2"})

	require.NotEqual(t, http.StatusNotFound, w.Code)
	assert.Empty(t, w.Header().Get("X-Request-ID"))
	assert.Empty(t, w.Header().Get("X-Trace-ID"))
	assert.Empty(t, rig.logger.accessLogs())
}
