package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type silentLogger struct{}

func (silentLogger) Debug(string, map[string]any) {}
func (silentLogger) Info(string, map[string]any)  {}
func (silentLogger) Warn(string, map[string]any)  {}
func (silentLogger) Error(string, map[string]any) {}

type warnLogger struct {
	silentLogger
	mu    sync.Mutex
	warns []string
}

func (l *warnLogger) Warn(msg string, _ map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}

func (l *warnLogger) warned() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.warns)
}

func grantsConfig(url string, timeout, ttl, bound time.Duration) *config.Config {
	return &config.Config{
		EntitlementsBaseURL: url, EntitlementsClientTimeout: timeout,
		EntitlementsCacheTTL: ttl, EntitlementsFreshnessBound: bound,
	}
}

func TestNewFeatureGrants_IsOffWithoutAnEntitlementsAddress(t *testing.T) {
	grants, err := newFeatureGrants(&config.Config{}, silentLogger{})

	require.NoError(t, err)
	assert.Nil(t, grants)
}

func TestNewFeatureGrants_ABadSettingStopsStartupInsteadOfTurningTheGrantOff(t *testing.T) {
	for name, cfg := range map[string]*config.Config{
		"a zero TTL":            grantsConfig("http://entitlements", time.Second, 0, time.Minute),
		"a bound below the TTL": grantsConfig("http://entitlements", time.Second, time.Minute, time.Second),
	} {
		t.Run(name, func(t *testing.T) {
			grants, err := newFeatureGrants(cfg, silentLogger{})

			require.Error(t, err)
			assert.Nil(t, grants)
		})
	}
}

func TestNewFeatureGrants_ReadsTheConfiguredServiceAndCachesTheAnswer(t *testing.T) {
	tenantID := uuid.New()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"tenant_id":"` + tenantID.String() + `","feature_flags":["dedicated_workflow_queue"]}`))
	}))
	t.Cleanup(srv.Close)
	grants, err := newFeatureGrants(grantsConfig(srv.URL, time.Second, time.Minute, 10*time.Minute), silentLogger{})
	require.NoError(t, err)
	require.NotNil(t, grants)

	for range 2 {
		has, err := grants.HasFeature(context.Background(), tenantID, "dedicated_workflow_queue")
		require.NoError(t, err)
		assert.True(t, has)
	}
	assert.EqualValues(t, 1, calls.Load())
}

// A short TTL with a long bound pins both settings and the logger: the held set is served after the TTL while the
// refresh fails, and the failure is logged. Swapping the two, or dropping the bound or the logger, fails this test.
func TestNewFeatureGrants_AFailingRefreshAfterTheTTLKeepsServingTheHeldSetAndIsLogged(t *testing.T) {
	tenantID := uuid.New()
	var calls atomic.Int32
	var failing atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"tenant_id":"` + tenantID.String() + `","feature_flags":["dedicated_workflow_queue"]}`))
	}))
	t.Cleanup(srv.Close)
	log := &warnLogger{}
	grants, err := newFeatureGrants(grantsConfig(srv.URL, time.Second, 5*time.Millisecond, time.Hour), log)
	require.NoError(t, err)
	has, err := grants.HasFeature(context.Background(), tenantID, "dedicated_workflow_queue")
	require.NoError(t, err)
	require.True(t, has)
	failing.Store(true)

	deadline := time.Now().Add(5 * time.Second)
	for log.warned() == 0 {
		require.True(t, time.Now().Before(deadline), "the TTL never expired, so no refresh ran")
		has, err = grants.HasFeature(context.Background(), tenantID, "dedicated_workflow_queue")
		require.NoError(t, err, "the held set is inside the bound")
		assert.True(t, has)
	}

	assert.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestNewFeatureGrants_ASlowEntitlementsServiceFailsFastAtTheClientTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release); srv.Close() })
	grants, err := newFeatureGrants(grantsConfig(srv.URL, 50*time.Millisecond, time.Minute, 10*time.Minute), silentLogger{})
	require.NoError(t, err)

	start := time.Now()
	_, err = grants.HasFeature(context.Background(), uuid.New(), "dedicated_workflow_queue")

	require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.Less(t, time.Since(start), 3*time.Second)
}

func TestNewInstanceService_ReceivesTheGrantsWhenTheAddressIsSet(t *testing.T) {
	log := &warnLogger{}
	base := service.InstanceService{Log: log}

	withGrants, err := newInstanceService(grantsConfig("http://entitlements", time.Second, time.Minute, time.Hour), log, base)
	require.NoError(t, err)
	assert.NotNil(t, withGrants.Grants, "the instance service decides the queue from the grant")
	assert.Same(t, log, withGrants.Log, "the other dependencies are kept")

	without, err := newInstanceService(&config.Config{}, log, base)
	require.NoError(t, err)
	assert.Nil(t, without.Grants, "no address leaves the queue to the registry alone")
}

func TestNewInstanceService_ABadGrantsSettingIsAnError(t *testing.T) {
	_, err := newInstanceService(grantsConfig("http://entitlements", time.Second, 0, time.Hour), silentLogger{}, service.InstanceService{})

	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "entitlements grants"))
}
