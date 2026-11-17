package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

type rdyDB struct{ healthy bool }

func (d rdyDB) Health(context.Context) inboundhttp.DBHealth {
	return inboundhttp.DBHealth{Healthy: d.healthy}
}

type rdyPinger struct{ err error }

func (p rdyPinger) Ping(context.Context) error { return p.err }

func rdyRouter(db rdyDB, cache, temporal rdyPinger, shuttingDown func() bool) http.Handler {
	gin.SetMode(gin.TestMode)
	return inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig:    gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test"},
		AppEnv:       "test",
		Handler:      handler.New(handler.Services{}),
		DB:           db,
		Cache:        cache,
		Temporal:     temporal,
		ShuttingDown: shuttingDown,
	}).Handler()
}

func rdyGet(h http.Handler, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
	return w
}

func TestReadyz_CacheOutage_StaysReadyAndReportsDegraded(t *testing.T) {
	h := rdyRouter(rdyDB{healthy: true}, rdyPinger{err: errors.New("valkey down")}, rdyPinger{}, nil)

	w := rdyGet(h, "/readyz")

	require.Equal(t, http.StatusOK, w.Code)
	body := hrfBody(t, w)
	assert.Equal(t, "OK", body["status"])
	assert.Equal(t, "degraded", body["cache"])
}

func TestReadyz_HealthyCache_ReportsNoCacheField(t *testing.T) {
	h := rdyRouter(rdyDB{healthy: true}, rdyPinger{}, rdyPinger{}, nil)

	w := rdyGet(h, "/readyz")

	require.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, hrfBody(t, w), "cache")
}

func TestReadyz_CacheOutageDoesNotHideAFailedDatabaseOrTemporal(t *testing.T) {
	cacheDown := rdyPinger{err: errors.New("valkey down")}
	cases := map[string]http.Handler{
		"db":       rdyRouter(rdyDB{healthy: false}, cacheDown, rdyPinger{}, nil),
		"temporal": rdyRouter(rdyDB{healthy: true}, cacheDown, rdyPinger{err: errors.New("temporal down")}, nil),
	}
	for failing, h := range cases {
		t.Run(failing, func(t *testing.T) {
			w := rdyGet(h, "/readyz")

			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			assert.Equal(t, "unreachable", hrfBody(t, w)[failing])
		})
	}
}

func TestReadyz_WhileShuttingDown_Answers503(t *testing.T) {
	var shuttingDown atomic.Bool
	h := rdyRouter(rdyDB{healthy: true}, rdyPinger{}, rdyPinger{}, shuttingDown.Load)
	require.Equal(t, http.StatusOK, rdyGet(h, "/readyz").Code)

	shuttingDown.Store(true)
	w := rdyGet(h, "/readyz")

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	body := hrfBody(t, w)
	assert.Equal(t, "unavailable", body["status"])
	assert.Equal(t, true, body["shutting_down"])
}

func TestHealthz_WhileShuttingDown_StaysAlive(t *testing.T) {
	var shuttingDown atomic.Bool
	shuttingDown.Store(true)
	h := rdyRouter(rdyDB{healthy: true}, rdyPinger{}, rdyPinger{}, shuttingDown.Load)

	assert.Equal(t, http.StatusOK, rdyGet(h, "/healthz").Code)
}

func TestRouter_PathWithAnAddedOrMissingTrailingSlash_Answers404NotARedirect(t *testing.T) {
	h := rdyRouter(rdyDB{healthy: true}, rdyPinger{}, rdyPinger{}, nil)
	cases := []struct{ method, path string }{
		{http.MethodGet, "/healthz/"},
		{http.MethodPost, "/api/v1/instances/"},
		{http.MethodGet, "/api/v1/tasks/"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

			require.Equal(t, http.StatusNotFound, w.Code)
			assert.Empty(t, w.Header().Get("Location"))
			assert.Equal(t, "NOT_FOUND", hrfBody(t, w)["code"])
		})
	}
}
