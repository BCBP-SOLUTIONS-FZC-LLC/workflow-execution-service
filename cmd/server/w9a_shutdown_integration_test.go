//go:build integration

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var errW9aValkeyClose = errors.New("valkey connection already gone")

// w9aOrder records which shutdown steps ran, in order.
type w9aOrder struct {
	mu     sync.Mutex
	events []string
}

func (o *w9aOrder) add(e string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *w9aOrder) all() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.events...)
}

type w9aSDK struct {
	client.Client
	order *w9aOrder
}

func (s w9aSDK) Close() { s.order.add("temporal close") }

type w9aCache struct {
	order *w9aOrder
	err   error
}

func (c w9aCache) Close() error {
	c.order.add("valkey close")
	return c.err
}

// w9aShutdownApp builds an app whose pools and outbox relay are real (one container) and whose other
// resources are fakes that record their shutdown. The relay is started, as in a running server.
func w9aShutdownApp(t *testing.T, pgDSN, appName string, cacheErr error) (*app, *w9aLogger, *w9aOrder) {
	t.Helper()
	ctx := context.Background()
	log := &w9aLogger{}
	order := &w9aOrder{}
	cfg := &config.Config{
		DatabaseURL: w9aWithApplicationName(t, pgDSN, appName), PGMaxConns: 4, PGMinConns: 1,
		OTELServiceName: "w9a", OutboxPollInterval: 100 * time.Millisecond, OutboxBatchSize: 50,
	}
	appPool, err := newAppPool(ctx, cfg, log)
	require.NoError(t, err)
	relayPool, err := newRelayPool(ctx, cfg, log)
	require.NoError(t, err)
	lockPool, err := newLockPool(ctx, cfg, log)
	require.NoError(t, err)
	relay, err := outbox.NewRunner(outbox.Config{Pool: relayPool, Publisher: &mock.Publisher{}, PollInterval: cfg.OutboxPollInterval, BatchSize: cfg.OutboxBatchSize})
	require.NoError(t, err)
	go func() { _ = relay.Start(context.Background()) }()
	select {
	case <-relay.Ready():
	case <-time.After(w9aWait):
		t.Fatal("the outbox relay never became ready")
	}
	_, cancel := context.WithCancel(context.Background())
	a := &app{
		cfg: cfg, log: log,
		appPool: appPool, relayPool: relayPool, lockPool: lockPool,
		cache:           w9aCache{order: order, err: cacheErr},
		httpServer:      &http.Server{ReadHeaderTimeout: time.Second},
		metricsServer:   &http.Server{ReadHeaderTimeout: time.Second},
		outboxRelay:     relay,
		cancelExporters: cancel,
		sdkClient:       w9aSDK{order: order},
		tracingShutdown: func() { order.add("tracing shutdown") },
	}
	t.Cleanup(func() {
		_ = relay.Stop()
		appPool.Close()
		relayPool.Close()
		lockPool.Close()
	})
	return a, log, order
}

func TestStopServers_AnHTTPShutdownThatRunsOutOfTimeIsLoggedAndEverythingElseStillCloses(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	const appName = "w9a_shutdown_http"
	a, log, order := w9aShutdownApp(t, pgDSN, appName, nil)
	require.Positive(t, w9aSessions(t, pgDSN, appName), "the three pools hold sessions before the stop")

	inFlight := make(chan struct{})
	release := make(chan struct{})
	a.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(inFlight)
		<-release
		w.WriteHeader(http.StatusOK)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = a.httpServer.Serve(ln) }()
	reqCtx, cancelReq := context.WithCancel(context.Background())
	t.Cleanup(func() {
		close(release)
		cancelReq()
		_ = a.httpServer.Close()
	})
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+ln.Addr().String()+"/slow", nil)
	require.NoError(t, err)
	go func() {
		if resp, doErr := http.DefaultClient.Do(req); doErr == nil {
			_ = resp.Body.Close()
		}
	}()
	select {
	case <-inFlight:
	case <-time.After(w9aWait):
		t.Fatal("the slow request never reached the handler")
	}
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	a.stopServers(expired)

	var shutdownErrors []string
	for _, r := range log.at("error") {
		if r.msg == "HTTP server shutdown error" {
			shutdownErrors = append(shutdownErrors, r.fields["error"].(string))
		}
	}
	assert.Equal(t, []string{context.DeadlineExceeded.Error()}, shutdownErrors, "the deadline error is logged once")
	for _, r := range log.at("error") {
		assert.NotContains(t, r.msg, "metrics", "only the HTTP server was still busy")
	}
	w9aEventuallySessions(t, pgDSN, appName, 0)
	assert.Equal(t, []string{"valkey close", "temporal close", "tracing shutdown"}, order.all(), "the later steps still ran, in order")
}

func TestStopServers_AValkeyCloseFailureIsLoggedAsAnErrorAndTemporalAndTracingStillClose(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	const appName = "w9a_shutdown_valkey"
	a, log, order := w9aShutdownApp(t, pgDSN, appName, errW9aValkeyClose)
	ctx, cancel := context.WithTimeout(context.Background(), w9aWait)
	defer cancel()

	a.stopServers(ctx)

	var closeErrors []string
	for _, r := range log.at("error") {
		if r.msg == "valkey close error" {
			closeErrors = append(closeErrors, r.fields["error"].(string))
		}
	}
	assert.Equal(t, []string{errW9aValkeyClose.Error()}, closeErrors, "logged once at Error level with the cause")
	assert.Empty(t, log.at("warn"), "a clean stop logs no warning")
	assert.Equal(t, []string{"valkey close", "temporal close", "tracing shutdown"}, order.all())
	w9aEventuallySessions(t, pgDSN, appName, 0)
}

func TestPoolBuilders_GiveEachPoolItsOwnDatabaseSettingsAndTenantInjection(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	cfg := &config.Config{
		DatabaseURL:            w9aWithApplicationName(t, pgDSN, "w9a_pool_app"),
		OutboxRelayDatabaseURL: w9aWithApplicationName(t, pgDSN, "w9a_pool_relay"),
		LockDatabaseURL:        w9aWithApplicationName(t, pgDSN, "w9a_pool_lock"),
		PGMaxConns:             6, PGMinConns: 1, PGSlowQueryThresholdMS: 1, OTELServiceName: "w9a",
	}
	appLog, relayLog, lockLog := &w9aLogger{}, &w9aLogger{}, &w9aLogger{}
	appPool, err := newAppPool(ctx, cfg, appLog)
	require.NoError(t, err)
	t.Cleanup(appPool.Close)
	relayPool, err := newRelayPool(ctx, cfg, relayLog)
	require.NoError(t, err)
	t.Cleanup(relayPool.Close)
	lockPool, err := newLockPool(ctx, cfg, lockLog)
	require.NoError(t, err)
	t.Cleanup(lockPool.Close)

	pools := []struct {
		name         string
		pool         *pgcommon.Pool
		log          *w9aLogger
		sessionName  string
		wantMaxConns int32
		wantTenant   bool
		wantSlowLog  bool
	}{
		{name: "app pool", pool: appPool, log: appLog, sessionName: "w9a_pool_app", wantMaxConns: 6, wantTenant: true, wantSlowLog: true},
		{name: "relay pool", pool: relayPool, log: relayLog, sessionName: "w9a_pool_relay", wantMaxConns: 6},
		{name: "lock pool", pool: lockPool, log: lockLog, sessionName: "w9a_pool_lock", wantMaxConns: 4},
	}
	tenant := "11111111-2222-3333-4444-555555555555"
	tenantCtx := pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenant})
	for _, tc := range pools {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantMaxConns, tc.pool.Stat().MaxConns())
			assert.Positive(t, w9aSessions(t, pgDSN, tc.sessionName), "the pool dialed its own DSN")

			var seen string
			require.NoError(t, tc.pool.WithConn(tenantCtx, func(ctx context.Context, conn *pgxpool.Conn) error {
				return conn.QueryRow(ctx, "SELECT COALESCE(current_setting('app.tenant_id', true), '')").Scan(&seen)
			}))
			if tc.wantTenant {
				assert.Equal(t, tenant, seen, "the caller's tenant is injected into the session")
			} else {
				assert.Empty(t, seen, "this pool injects no tenant")
			}

			before := len(w9aSlowQueryWarnings(tc.log))
			require.NoError(t, tc.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				_, execErr := conn.Exec(ctx, "SELECT pg_sleep(0.05)")
				return execErr
			}))
			after := len(w9aSlowQueryWarnings(tc.log))
			if tc.wantSlowLog {
				// The tracer logs at most one slow query per second, so a slow
				// query just before can hide this one; any warning proves the
				// threshold is set, and a 50ms query is always over 1ms.
				assert.Positive(t, after, "a 50ms query is over the 1ms threshold")
			} else {
				assert.Equal(t, before, after, "this pool has no slow-query threshold")
				assert.Zero(t, after)
			}
		})
	}
}

func w9aSlowQueryWarnings(l *w9aLogger) []w9aLogEntry {
	var out []w9aLogEntry
	for _, r := range l.at("warn") {
		if r.msg == "slow query" {
			out = append(out, r)
		}
	}
	return out
}
