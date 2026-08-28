package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

var testGinConfig = gincommon.Config{ServiceName: "execution-service-test"}

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

type fakeDBPinger struct {
	health DBHealth
}

func (f fakeDBPinger) Health(context.Context) DBHealth { return f.health }

func testRouter(db DBPinger, cache, temporal Pinger) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := NewRouter(RouterConfig{
		GinConfig: testGinConfig,
		AppEnv:    "test",
		Handler:   handler.New(handler.Services{}),
		DB:        db, Cache: cache, Temporal: temporal,
	})
	return r.engine
}

func TestReadyz_AllHealthy_Returns200(t *testing.T) {
	r := testRouter(fakeDBPinger{health: DBHealth{Healthy: true}}, fakePinger{}, fakePinger{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReadyz_DBUnhealthy_Returns503(t *testing.T) {
	r := testRouter(fakeDBPinger{health: DBHealth{Healthy: false}}, fakePinger{}, fakePinger{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestReadyz_CacheUnreachable_Returns503(t *testing.T) {
	r := testRouter(fakeDBPinger{health: DBHealth{Healthy: true}}, fakePinger{err: errors.New("valkey down")}, fakePinger{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestReadyz_TemporalUnreachable_Returns503(t *testing.T) {
	r := testRouter(fakeDBPinger{health: DBHealth{Healthy: true}}, fakePinger{}, fakePinger{err: errors.New("temporal down")})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestNewRouter_Healthz(t *testing.T) {
	r := testRouter(fakeDBPinger{}, fakePinger{}, fakePinger{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestNewRouter_InternalRoutesRequireToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(RouterConfig{
		GinConfig:        testGinConfig,
		AppEnv:           "test",
		InternalAPIToken: "secret",
		Handler:          handler.New(handler.Services{}),
		DB:               fakeDBPinger{}, Cache: fakePinger{}, Temporal: fakePinger{},
	})

	w := httptest.NewRecorder()
	router.engine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/events", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code, "a missing x-internal-token must be rejected before reaching the handler")
}

func TestNewRouter_DevMode_MountsAsyncAPIRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(RouterConfig{
		GinConfig: testGinConfig,
		AppEnv:    "dev",
		Handler:   handler.New(handler.Services{}),
		DB:        fakeDBPinger{}, Cache: fakePinger{}, Temporal: fakePinger{},
	})

	w := httptest.NewRecorder()
	router.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))

	assert.NotEqual(t, http.StatusNotFound, w.Code, "/asyncapi must be mounted in dev mode")
}

func TestNewRouter_NonDevMode_NoAsyncAPIRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(RouterConfig{
		GinConfig: testGinConfig,
		AppEnv:    "production",
		Handler:   handler.New(handler.Services{}),
		DB:        fakeDBPinger{}, Cache: fakePinger{}, Temporal: fakePinger{},
	})

	w := httptest.NewRecorder()
	router.engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))

	assert.Equal(t, http.StatusNotFound, w.Code, "/asyncapi must not be mounted outside dev mode")
}

func TestRouter_Handler_ReturnsUnderlyingEngine(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(RouterConfig{
		GinConfig: testGinConfig,
		AppEnv:    "test",
		Handler:   handler.New(handler.Services{}),
		DB:        fakeDBPinger{}, Cache: fakePinger{}, Temporal: fakePinger{},
	})

	require.NotNil(t, router.Handler())
}
