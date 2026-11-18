//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestNewApp_AnOutboxRunnerThatCannotBeBuiltFailsWithOutboxRunnerAndReleasesThePools(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	const appName = "w9a_runner"
	taggedDSN := w9aWithApplicationName(t, pgDSN, appName)

	t.Run("control: the default batch size builds the runner and the app starts", func(t *testing.T) {
		a, err := newApp(w9aAppConfig(t, taggedDSN, temporalAddr, nil))

		require.NoError(t, err)
		var once sync.Once
		release := func() { once.Do(func() { w9aCloseApp(a) }) }
		t.Cleanup(release)
		assert.NotNil(t, a.outboxRelay)
		release()
		w9aEventuallySessions(t, pgDSN, appName, 0)
	})

	cases := []struct {
		name string
		env  map[string]string
	}{
		{name: "a batch so large that its publish budget outlasts the claim lease", env: map[string]string{"OUTBOX_BATCH_SIZE": "1000000"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newApp(w9aAppConfig(t, taggedDSN, temporalAddr, tc.env))

			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), "outbox runner: ")
			assert.Contains(t, err.Error(), "ClaimLeaseDuration", "the cause from the library comes through")
			w9aEventuallySessions(t, pgDSN, appName, 0)
		})
	}
}

func TestNewApp_StartsWithoutContactingTheOutboundServicesAndReadyzAnswersOKWithValkeyDegraded(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	definition, eligibility, tokens, entitlements := w9aNewCounter(t), w9aNewCounter(t), w9aNewCounter(t), w9aNewCounter(t)
	cfg := w9aAppConfig(t, pgDSN, temporalAddr, map[string]string{
		"DEFINITION_SERVICE_URL": definition.url(),
		"ELIGIBILITY_BASE_URL":   eligibility.url(),
		"TOKEN_SERVICE_BASE_URL": tokens.url(),
		"ENTITLEMENTS_BASE_URL":  entitlements.url(),
	})

	a, err := newApp(cfg)

	require.NoError(t, err)
	t.Cleanup(func() { w9aCloseApp(a) })
	rec := httptest.NewRecorder()
	a.httpServer.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Status string `json:"status"`
		Cache  string `json:"cache"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "OK", body.Status)
	assert.Equal(t, "degraded", body.Cache, "the unreachable Valkey is reported without failing readiness")
	for name, c := range map[string]*w9aCounter{
		"definition": definition, "eligibility": eligibility, "token service": tokens, "entitlements": entitlements,
	} {
		assert.Zero(t, c.conns.Load(), "%s must not be contacted at startup or by readiness", name)
	}
}

func TestNewApp_TheRetentionSweeperPrunesOutboxRowsAndProcessedEventsAtTheConfiguredAges(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	cfg := w9aAppConfig(t, pgDSN, temporalAddr, map[string]string{
		"OUTBOX_RETENTION":          "61320h",
		"PROCESSED_EVENT_RETENTION": "168h",
		"RETENTION_SWEEP_BATCH":     "1000",
	})
	a, err := newApp(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { w9aCloseApp(a) })

	ctx := context.Background()
	ids := map[string]uuid.UUID{}
	w9aWithConn(t, pgDSN, func(ctx context.Context, conn *pgxpool.Conn) error {
		insertOutbox := func(label, publishedAge string) error {
			ids[label] = uuid.New()
			published := "NULL"
			if publishedAge != "" {
				published = "now() - interval '" + publishedAge + "'"
			}
			_, err := conn.Exec(ctx, "INSERT INTO outbox_events (id, event_type, payload, created_at, published_at) VALUES ($1, 'w9a.event', '{}', now() - interval '9 years', "+published+")", ids[label])
			return err
		}
		insertProcessed := func(label, consumer, age string) error {
			ids[label] = uuid.New()
			_, err := conn.Exec(ctx, "INSERT INTO processed_events (event_id, consumer, processed_at) VALUES ($1, $2, now() - interval '"+age+"')", ids[label], consumer)
			return err
		}
		return errors.Join(
			insertOutbox("outbox published 8 years ago", "8 years"),
			insertOutbox("outbox published 6 years ago", "6 years"),
			insertOutbox("outbox never published", ""),
			insertProcessed("membership 8 days ago", port.ConsumerMembership, "8 days"),
			insertProcessed("user 8 days ago", port.ConsumerUser, "8 days"),
			insertProcessed("membership 6 days ago", port.ConsumerMembership, "6 days"),
		)
	})

	require.NoError(t, a.retentionSweeper.Tick(ctx))

	exists := func(table, column, label string) bool {
		var found bool
		w9aWithConn(t, pgDSN, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+table+" WHERE "+column+" = $1)", ids[label]).Scan(&found)
		})
		return found
	}
	assert.False(t, exists("outbox_events", "id", "outbox published 8 years ago"), "published beyond the outbox retention")
	assert.True(t, exists("outbox_events", "id", "outbox published 6 years ago"), "published inside the outbox retention")
	assert.True(t, exists("outbox_events", "id", "outbox never published"), "an unpublished row is never pruned")
	assert.False(t, exists("processed_events", "event_id", "membership 8 days ago"), "processed beyond the retention")
	assert.False(t, exists("processed_events", "event_id", "user 8 days ago"), "every consumer's rows are pruned")
	assert.True(t, exists("processed_events", "event_id", "membership 6 days ago"), "processed inside the retention")
}

// w9aSignals counts calls to a fake and lets a test wait for the first one.
type w9aSignals struct {
	mu    sync.Mutex
	calls int
	first chan struct{}
}

func w9aNewSignals() *w9aSignals { return &w9aSignals{first: make(chan struct{})} }

func (s *w9aSignals) hit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		close(s.first)
	}
}

func (s *w9aSignals) waitFirst(t *testing.T, what string) {
	t.Helper()
	select {
	case <-s.first:
	case <-time.After(w9aWait):
		t.Fatalf("%s never ran", what)
	}
}

type w9aRLSLog struct {
	port.RLSViolationLog
	horizon *w9aSignals
}

func (l w9aRLSLog) Horizon(_ context.Context, _ time.Duration) (time.Time, error) {
	l.horizon.hit()
	return time.Now(), nil
}

type w9aStatsReader struct{ read *w9aSignals }

func (r w9aStatsReader) Read(context.Context) (domain.OperationalStats, error) {
	r.read.hit()
	return domain.OperationalStats{}, nil
}

type w9aOutboxPruner struct{ pruned *w9aSignals }

func (p w9aOutboxPruner) PrunePublished(context.Context, time.Duration, int) (int64, error) {
	p.pruned.hit()
	return 0, nil
}

type w9aInboxPruner struct{ pruned *w9aSignals }

func (p w9aInboxPruner) PruneBatch(context.Context, time.Duration, int) (int64, error) {
	p.pruned.hit()
	return 0, nil
}

type w9aRetentionRepo struct {
	port.TenantRetentionRepository
	listed *w9aSignals
}

func (r w9aRetentionRepo) ListDue(context.Context, time.Time, int) ([]port.TenantPurgeDue, error) {
	r.listed.hit()
	return nil, nil
}

func TestStartServers_LaunchesTheThreeLoopsEveryTimeAndThePurgerOnlyWhenItIsConfigured(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)

	cases := []struct {
		name       string
		purge      string
		wantPurger bool
	}{
		{name: "purging enabled", purge: "true", wantPurger: true},
		{name: "purging disabled", purge: "false", wantPurger: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := w9aAppConfig(t, pgDSN, temporalAddr, map[string]string{"TENANT_PURGE_ENABLED": tc.purge})
			a, err := newApp(cfg)
			require.NoError(t, err)
			t.Cleanup(func() {
				if a.cancelExporters != nil {
					a.cancelExporters()
					_ = a.httpServer.Close()
					_ = a.metricsServer.Close()
					_ = a.outboxRelay.Stop()
				}
				w9aCloseApp(a)
			})
			require.Equal(t, tc.wantPurger, a.tenantPurger != nil, "newApp builds the purger only when it is enabled")

			rls, stats, outboxPruned, inboxPruned, listed := w9aNewSignals(), w9aNewSignals(), w9aNewSignals(), w9aNewSignals(), w9aNewSignals()
			outboxPruner := w9aOutboxPruner{pruned: outboxPruned}
			retention := w9aRetentionRepo{listed: listed}
			a.rlsExporter = service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
				Log: w9aRLSLog{horizon: rls}, Record: func(string, float64) {}, Interval: time.Hour, Retention: time.Hour, Logger: a.log,
			})
			a.statsSampler = service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
				Stats: w9aStatsReader{read: stats}, Record: func(domain.OperationalStats) {}, Interval: time.Hour, Logger: a.log,
			})
			a.retentionSweeper = service.NewRetentionSweeper(service.RetentionSweeperDeps{
				Outbox: outboxPruner, ProcessedEvents: w9aInboxPruner{pruned: inboxPruned}, OutboxRetention: time.Hour, ProcessedEventRetention: time.Hour,
				Batch: cfg.RetentionSweepBatch, Interval: time.Hour, Record: func(string, int64) {}, Logger: a.log,
			})
			if tc.wantPurger {
				a.tenantPurger = service.NewTenantPurger(service.TenantPurgerDeps{
					Retention: retention, Data: nil, Batch: cfg.RetentionSweepBatch, Interval: time.Hour,
					Record: func(string, int64) {}, Failed: func() {}, Logger: a.log,
				})
			}
			a.httpServer = &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second}
			a.metricsServer = &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second}
			_ = a.startServers()

			rls.waitFirst(t, "the RLS violation exporter")
			stats.waitFirst(t, "the operational stats sampler")
			outboxPruned.waitFirst(t, "the retention sweeper's outbox pruning")
			inboxPruned.waitFirst(t, "the retention sweeper's processed-event pruning")
			select {
			case <-a.outboxRelay.Ready():
			case <-time.After(w9aWait):
				t.Fatal("the outbox relay never became ready")
			}
			if tc.wantPurger {
				listed.waitFirst(t, "the tenant purger")
			}
		})
	}
}
