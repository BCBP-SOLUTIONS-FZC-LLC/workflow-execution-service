//go:build integration

package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const w9aWait = 30 * time.Second

// w9aTemporalAddr starts a dev Temporal server and returns its host:port once it answers health checks.
func w9aTemporalAddr(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "temporalio/temporal:latest",
			Cmd:          []string{"server", "start-dev", "--ip", "0.0.0.0"},
			ExposedPorts: []string{"7233/tcp"},
			WaitingFor:   wait.ForListeningPort("7233/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if container != nil {
		t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	}
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	deadline := time.Now().Add(90 * time.Second)
	var addr string
	for {
		p, portErr := container.MappedPort(ctx, "7233")
		if portErr == nil {
			addr = host + ":" + p.Port()
			sdk, dialErr := client.Dial(client.Options{HostPort: addr})
			if dialErr == nil {
				healthCtx, cancelHealth := context.WithTimeout(ctx, 5*time.Second)
				_, healthErr := sdk.CheckHealth(healthCtx, &client.CheckHealthRequest{})
				cancelHealth()
				sdk.Close()
				if healthErr == nil {
					return addr
				}
			}
		}
		require.True(t, time.Now().Before(deadline), "the Temporal server never became healthy")
		time.Sleep(time.Second)
	}
}

// w9aWithApplicationName tags every connection a DSN opens so the test can count the sessions the server holds.
func w9aWithApplicationName(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// w9aWithConn opens a pgcommon pool on dsn for one callback and closes it afterwards.
func w9aWithConn(t *testing.T, dsn string, fn func(ctx context.Context, conn *pgxpool.Conn) error) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn})
	require.NoError(t, err)
	defer pool.Close()
	require.NoError(t, pool.WithConn(ctx, fn))
}

func w9aSessions(t *testing.T, dsn, name string) int {
	t.Helper()
	var n int
	w9aWithConn(t, dsn, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", name).Scan(&n)
	})
	return n
}

// w9aEventuallySessions polls until the count reaches want, or fails after a bounded wait.
func w9aEventuallySessions(t *testing.T, dsn, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(w9aWait)
	for {
		got := w9aSessions(t, dsn, name)
		if got == want {
			return
		}
		require.True(t, time.Now().Before(deadline), "sessions named %s = %d, want %d", name, got, want)
		time.Sleep(100 * time.Millisecond)
	}
}

// w9aCounter is an HTTP server that answers 200 and counts every connection made to it.
type w9aCounter struct {
	srv   *httptest.Server
	conns atomic.Int64
}

func w9aNewCounter(t *testing.T) *w9aCounter {
	t.Helper()
	c := &w9aCounter{}
	c.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	c.srv.Config.ConnState = c.track
	c.srv.Start()
	t.Cleanup(c.srv.Close)
	return c
}

func (c *w9aCounter) track(_ net.Conn, state http.ConnState) {
	if state == http.StateNew {
		c.conns.Add(1)
	}
}

func (c *w9aCounter) url() string { return c.srv.URL }

// w9aConfigVariables is every variable config.Load reads, cleared first so the machine's own settings never leak in.
var w9aConfigVariables = []string{
	"APP_ENV", "AWS_ENDPOINT_URL", "AWS_REGION", "AWS_USE_STUB", "BUILD_VERSION", "CONNECTOR_ALIAS_FETCH_TIMEOUT",
	"CONNECTOR_ALIAS_REFRESH_INTERVAL", "CONNECTOR_AUDIT_RETENTION", "CONNECTOR_COMPLETION_TIMEOUT", "CONNECTOR_DOCS_BUCKET",
	"CONNECTOR_POOL_SIZE_CHAT_NOTIFY", "CONNECTOR_POOL_SIZE_DOCUMENT_EXTRACT", "CONNECTOR_POOL_SIZE_REST_CALL",
	"CONNECTOR_POOL_SIZE_SEND_EMAIL", "CONNECTOR_POOL_SIZE_SQL_QUERY", "CONNECTOR_POOL_SIZE_STORAGE",
	"CONNECTOR_RETRY_INITIAL_BACKOFF", "CONNECTOR_RETRY_MAX_ATTEMPTS", "CONNECTOR_RETRY_MAX_BACKOFF",
	"CONNECTOR_STREAM_BATCH_SIZE", "CONNECTOR_STREAM_BLOCK_TIMEOUT", "CONNECTOR_STREAM_CLAIM_MIN_IDLE",
	"CONNECTOR_STREAM_CONSUMER_NAME", "CONNECTOR_STREAM_DEAD_LETTER_KEY", "CONNECTOR_STREAM_DEAD_LETTER_RETENTION",
	"CONNECTOR_STREAM_GROUP", "CONNECTOR_STREAM_KEY", "CONNECTOR_STREAM_MAX_DELIVERIES", "CONNECTOR_TIMEOUT_CHAT_NOTIFY",
	"CONNECTOR_TIMEOUT_DOCUMENT_EXTRACT", "CONNECTOR_TIMEOUT_REST_CALL", "CONNECTOR_TIMEOUT_SEND_EMAIL",
	"CONNECTOR_TIMEOUT_SQL_QUERY", "CONNECTOR_TIMEOUT_STORAGE", "DATABASE_URL", "DEFINITION_CLIENT_TIMEOUT",
	"DEFINITION_SERVICE_URL", "ELIGIBILITY_BASE_URL", "ELIGIBILITY_CLIENT_TIMEOUT", "ENTITLEMENTS_BASE_URL",
	"ENTITLEMENTS_CACHE_TTL", "ENTITLEMENTS_CLIENT_TIMEOUT", "ENTITLEMENTS_FRESHNESS_BOUND", "EXECUTION_SERVICE_INTERNAL_ADDR",
	"GLUE_REGISTRY_NAME", "GLUE_SCHEMA_CACHE_TTL", "GLUE_SCHEMA_MAX_STALENESS", "HTTP_PORT", "IDEMPOTENCY_TTL",
	"LOCK_DATABASE_URL", "METRICS_PORT", "MIGRATION_DATABASE_URL", "OPENBAO_ADDR", "OPENBAO_MOUNT", "OPENBAO_TIMEOUT",
	"OPENBAO_TOKEN", "OPERATIONAL_STATS_INTERVAL", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_INSECURE",
	"OTEL_SERVICE_NAME", "OTEL_TRACES_EXPORTER", "OTEL_TRACES_SAMPLER_RATIO", "OUTBOX_BATCH_SIZE", "OUTBOX_POLL_INTERVAL",
	"OUTBOX_RELAY_DATABASE_URL", "OUTBOX_RETENTION", "PG_BOUNCER_MODE", "PG_MAX_CONNS", "PG_MIN_CONNS",
	"PG_SLOW_QUERY_THRESHOLD_MS", "PROCESSED_EVENT_RETENTION", "QUEUE_TOPOLOGY_POLL_INTERVAL", "RETENTION_SWEEP_BATCH",
	"RETENTION_SWEEP_INTERVAL", "RLS_VIOLATION_EXPORT_INTERVAL", "RLS_VIOLATION_RETENTION", "SHUTDOWN_DRAIN_DELAY",
	"SNS_TOPIC_ARN", "START_REFUSED_STATUSES", "TEMPORAL_HOST_PORT", "TEMPORAL_NAMESPACE", "TEMPORAL_TASK_QUEUE",
	"TEMPORAL_TLS_CERT_PATH", "TEMPORAL_TLS_KEY_PATH", "TENANT_HISTORY_RETENTION", "TENANT_PURGE_ENABLED",
	"TOKEN_SERVICE_BASE_URL", "TOKEN_SERVICE_CLIENT_TIMEOUT", "VALKEY_ADDR", "VALKEY_DIAL_TIMEOUT", "VALKEY_PASSWORD",
	"VALKEY_READ_TIMEOUT", "VALKEY_WRITE_TIMEOUT", "WORKER_HEALTH_PORT", "WORKER_METRICS_PORT", "WORKFLOW_CACHE_SIZE",
}

// w9aAppConfig loads the service configuration from a controlled environment.
func w9aAppConfig(t *testing.T, pgDSN, temporalAddr string, env map[string]string) *config.Config {
	t.Helper()
	base := map[string]string{
		"APP_ENV":                "test",
		"BUILD_VERSION":          "test",
		"TEMPORAL_NAMESPACE":     "default",
		"DATABASE_URL":           pgDSN,
		"TEMPORAL_HOST_PORT":     temporalAddr,
		"VALKEY_ADDR":            "127.0.0.1:1",
		"VALKEY_DIAL_TIMEOUT":    "200ms",
		"VALKEY_READ_TIMEOUT":    "200ms",
		"VALKEY_WRITE_TIMEOUT":   "200ms",
		"HTTP_PORT":              "18081",
		"METRICS_PORT":           "19092",
		"SHUTDOWN_DRAIN_DELAY":   "0s",
		"PG_MAX_CONNS":           "4",
		"PG_MIN_CONNS":           "1",
		"DEFINITION_SERVICE_URL": "http://127.0.0.1:1",
		"ELIGIBILITY_BASE_URL":   "http://127.0.0.1:1",
		"TOKEN_SERVICE_BASE_URL": "http://127.0.0.1:1",
		"OUTBOX_POLL_INTERVAL":   "100ms",
	}
	for k, v := range env {
		base[k] = v
	}
	for _, k := range w9aConfigVariables {
		t.Setenv(k, "")
		require.NoError(t, os.Unsetenv(k))
	}
	for k, v := range base {
		t.Setenv(k, v)
	}
	cfg, err := config.Load()
	require.NoError(t, err)
	return cfg
}

// w9aCloseApp releases what a started-but-never-served app holds.
func w9aCloseApp(a *app) {
	a.relayPool.Close()
	a.appPool.Close()
	a.inboxPool.Close()
	a.lockPool.Close()
	_ = a.cache.Close()
	a.sdkClient.Close()
	a.tracingShutdown()
}

func TestNewApp_AFailureAfterTheDatabasesOpenReleasesEveryPoolAndTheProcessExitsOne(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	const appName = "w9a_unwind"
	taggedDSN := w9aWithApplicationName(t, pgDSN, appName)

	t.Run("control: a start that succeeds holds sessions open", func(t *testing.T) {
		cfg := w9aAppConfig(t, taggedDSN, temporalAddr, nil)

		a, err := newApp(cfg)

		require.NoError(t, err)
		var once sync.Once
		release := func() { once.Do(func() { w9aCloseApp(a) }) }
		t.Cleanup(release)
		assert.Positive(t, w9aSessions(t, pgDSN, appName), "the app, relay and lock pools are open")
		release()
		w9aEventuallySessions(t, pgDSN, appName, 0)
	})

	cases := []struct {
		name    string
		env     map[string]string
		mutate  func(*config.Config)
		wantErr string
	}{
		{
			name:    "an unreadable Temporal client certificate",
			env:     map[string]string{"TEMPORAL_TLS_CERT_PATH": "/nonexistent/w9a.crt", "TEMPORAL_TLS_KEY_PATH": "/nonexistent/w9a.key"},
			wantErr: "temporal client certificate",
		},
		{
			name:    "an entitlements cache time of zero",
			env:     map[string]string{"ENTITLEMENTS_BASE_URL": "http://127.0.0.1:1"},
			mutate:  func(c *config.Config) { c.EntitlementsCacheTTL = 0 },
			wantErr: "entitlements grants",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := w9aAppConfig(t, taggedDSN, temporalAddr, tc.env)
			if tc.mutate != nil {
				tc.mutate(cfg)
			}

			got, err := newApp(cfg)

			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), tc.wantErr)
			w9aEventuallySessions(t, pgDSN, appName, 0)
		})
	}

	t.Run("a relay role that cannot bypass row security in a non-dev environment", func(t *testing.T) {
		w9aExec(t, pgDSN,
			"DROP ROLE IF EXISTS w9a_relay_plain",
			"CREATE ROLE w9a_relay_plain LOGIN PASSWORD 'w9a-plain-pw' NOBYPASSRLS",
			"GRANT CONNECT ON DATABASE testdb TO w9a_relay_plain")
		u, err := url.Parse(taggedDSN)
		require.NoError(t, err)
		u.User = url.UserPassword("w9a_relay_plain", "w9a-plain-pw")
		cfg := w9aAppConfig(t, taggedDSN, temporalAddr, map[string]string{"OUTBOX_RELAY_DATABASE_URL": u.String()})

		got, err := newApp(cfg)

		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "BYPASSRLS")
		w9aEventuallySessions(t, pgDSN, appName, 0)
	})

	t.Run("the built binary prints the cmd/server prefix and exits one", func(t *testing.T) {
		res := w9aRunServer(t, []string{
			"APP_ENV=dev",
			"DATABASE_URL=" + taggedDSN,
			"TEMPORAL_HOST_PORT=" + temporalAddr,
			"TEMPORAL_TLS_CERT_PATH=/nonexistent/w9a.crt",
			"TEMPORAL_TLS_KEY_PATH=/nonexistent/w9a.key",
			"VALKEY_ADDR=127.0.0.1:1",
			"VALKEY_DIAL_TIMEOUT=200ms",
			"DEFINITION_SERVICE_URL=http://127.0.0.1:1",
			"ELIGIBILITY_BASE_URL=http://127.0.0.1:1",
			"TOKEN_SERVICE_BASE_URL=http://127.0.0.1:1",
			"SHUTDOWN_DRAIN_DELAY=0s",
		})

		assert.Equal(t, 1, res.exitCode)
		assert.Contains(t, res.stderr, "cmd/server: temporal client certificate")
		assert.NotContains(t, res.combined(), "server starting")
		w9aEventuallySessions(t, pgDSN, appName, 0)
	})
}
