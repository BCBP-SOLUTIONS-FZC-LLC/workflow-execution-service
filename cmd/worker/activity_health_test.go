package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func wkactServe(t *testing.T, srv *http.Server, method, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	return rec.Code, string(body)
}

func TestNewHealthServer_Healthz_AnswersOKWithAnEmptyBodyWithoutTouchingDatabaseOrTemporal(t *testing.T) {
	t.Parallel()

	// A nil pool and a nil Temporal client would panic if /healthz looked at either.
	srv := newHealthServer("127.0.0.1:0", nil, nil)

	cases := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{"GET", http.MethodGet, "/healthz", http.StatusOK},
		{"POST", http.MethodPost, "/healthz", http.StatusOK},
		{"unknown path", http.MethodGet, "/health", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, body := wkactServe(t, srv, tc.method, tc.path)
			assert.Equal(t, tc.wantStatus, status)
			if tc.wantStatus == http.StatusOK {
				assert.Empty(t, body)
			}
		})
	}
}

func TestNewServers_ListenOnTheGivenAddressWithTheDocumentedTimeouts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                          string
		srv                           *http.Server
		addr                          string
		readHeader, read, write, idle time.Duration
	}{
		{"health", newHealthServer("127.0.0.1:18081", nil, nil), "127.0.0.1:18081", 5 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second},
		{"metrics", newMetricsServer("127.0.0.1:19091"), "127.0.0.1:19091", 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.addr, tc.srv.Addr)
			assert.Equal(t, tc.readHeader, tc.srv.ReadHeaderTimeout)
			assert.Equal(t, tc.read, tc.srv.ReadTimeout)
			assert.Equal(t, tc.write, tc.srv.WriteTimeout)
			assert.Equal(t, tc.idle, tc.srv.IdleTimeout)
		})
	}
}

func TestNewMetricsServer_Metrics_ServesTheServiceCollectorsAndNothingElse(t *testing.T) {
	obstest.Init(t)
	observability.WorkerActiveQueues.Set(3)
	srv := newMetricsServer("127.0.0.1:0")

	status, body := wkactServe(t, srv, http.MethodGet, "/metrics")

	assert.Equal(t, http.StatusOK, status)
	assert.Regexp(t, regexp.MustCompile(`(?m)^# TYPE workflow_execution_service_worker_active_queues gauge$`), body)
	assert.Regexp(t, regexp.MustCompile(`(?m)^workflow_execution_service_worker_active_queues(\{[^}]*\})? 3$`), body)

	for _, path := range []string{"/", "/metricsz", "/healthz"} {
		status, _ := wkactServe(t, srv, http.MethodGet, path)
		assert.Equal(t, http.StatusNotFound, status, path)
	}
}
