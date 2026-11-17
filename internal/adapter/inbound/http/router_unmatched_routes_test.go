package http_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_UnmatchedInternalPath_Returns404NotAnAuthFailure(t *testing.T) {
	rig := hrfNewRig(t, "test")

	w := rig.do(http.MethodGet, "/api/v1/internal/does-not-exist", nil)

	require.Equal(t, http.StatusNotFound, w.Code)
	body := hrfBody(t, w)
	assert.Equal(t, "NOT_FOUND", body["code"])
	assert.Equal(t, "/api/v1/internal/does-not-exist", body["instance"])
}

func TestRouter_TrailingSlashOnGetRoute_Answers404Problem(t *testing.T) {
	rig := hrfNewRig(t, "test")

	w := rig.do(http.MethodGet, "/healthz/", nil)

	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Empty(t, w.Header().Get("Location"))
	assert.Equal(t, "NOT_FOUND", hrfBody(t, w)["code"])
}

func TestRouter_TrailingSlashOnPostRoute_Answers404BeforeAuthAndHandler(t *testing.T) {
	rig := hrfNewRig(t, "test")
	claimsBefore := rig.cache.claimCount()

	w := rig.do(http.MethodPost, "/api/v1/instances/", map[string]string{"Idempotency-Key": "hrf-trailing-slash"})

	assert.Equal(t, http.StatusNotFound, w.Code, "not 401: no route matches, so no auth middleware runs")
	assert.Empty(t, w.Header().Get("Location"))
	assert.Equal(t, claimsBefore, rig.cache.claimCount(), "the idempotency wrapper and handler must not run")
}

func TestRouter_MethodNotRegisteredForPath_Returns404NotAFallbackToGet(t *testing.T) {
	cases := []struct {
		name   string
		method string
	}{
		{name: "head", method: http.MethodHead},
		{name: "options", method: http.MethodOptions},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := hrfNewRig(t, "test")

			w := rig.do(tc.method, "/healthz", nil)

			require.Equal(t, http.StatusNotFound, w.Code)
			body := hrfBody(t, w)
			assert.Equal(t, "NOT_FOUND", body["code"])
			assert.NotEqual(t, "ok", body["status"], "the health handler must not have answered")
		})
	}
}

func TestRouter_ScannerPath_UsesUnknownRouteLabelInMetricsAndAccessLog(t *testing.T) {
	rig := hrfNewRig(t, "test")
	probe := uuid.NewString()
	path := "/scanner/" + probe
	before := hrfRequestsTotal(t, http.MethodGet, "unknown", "4xx")

	w := rig.do(http.MethodGet, path, nil)

	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "NOT_FOUND", hrfBody(t, w)["code"])
	assert.Equal(t, before+1, hrfRequestsTotal(t, http.MethodGet, "unknown", "4xx"))
	assert.False(t, hrfAnyMetricLabelContains(t, probe), "the raw path must never become a metric label")
	logs := rig.logger.accessLogs()
	require.Len(t, logs, 1)
	assert.Equal(t, "unknown", logs[0].fields["path"])
	assert.Equal(t, "warn", logs[0].level)
}

func TestRouter_DevTrailingSlashOnAsyncAPI_Answers404(t *testing.T) {
	rig := hrfNewRig(t, "dev")

	w := rig.do(http.MethodGet, "/asyncapi/", nil)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Empty(t, w.Header().Get("Location"))
}

func TestRouter_DevPathsBelowAsyncAPI_Return404Problem(t *testing.T) {
	for _, path := range []string{"/asyncapi/anything", "/asyncapi/static/asyncapi.js"} {
		t.Run(path, func(t *testing.T) {
			rig := hrfNewRig(t, "dev")

			w := rig.do(http.MethodGet, path, nil)

			require.Equal(t, http.StatusNotFound, w.Code)
			body := hrfBody(t, w)
			assert.Equal(t, "NOT_FOUND", body["code"])
			assert.Equal(t, "https://errors.bcbp.io/execution/not-found", body["type"])
			assert.Equal(t, path, body["instance"])
		})
	}
}
