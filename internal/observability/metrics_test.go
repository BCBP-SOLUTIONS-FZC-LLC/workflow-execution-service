package observability_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// scrape registers metrics and returns the default registry's /metrics body.
func scrape(t *testing.T) string {
	t.Helper()

	observability.Register()

	srv := httptest.NewServer(promhttp.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL) //nolint:noctx,gosec // test-only, fixed httptest URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

// TestRegister_EveryMetricIsScrapable records one representative sample on
// every metric, then confirms each appears in a scrape. A *Vec collector
// with an open-ended label and zero recorded observations emits nothing at
// all (not even HELP/TYPE) until something calls WithLabelValues — that's
// normal client_golang behavior, not something Register should work around
// — so this test proves scrapability the same way real call sites will
// eventually trigger it, rather than asserting on incidental zero-value
// output.
func TestRegister_EveryMetricIsScrapable(t *testing.T) {
	observability.Register()

	observability.RecordVersionConflictTotal.WithLabelValues("workflow_instance").Inc()
	observability.DBTxRetryTotal.Inc()
	observability.InternalEventsIngestTotal.WithLabelValues("delegation.started", "ok").Inc()
	observability.DelegationRerouteDurationSeconds.Observe(1.5)
	observability.TaskSignalDurationSeconds.WithLabelValues("claim").Observe(0.1)
	observability.InstanceStartDurationSeconds.Observe(0.5)
	observability.WorkflowActivityDurationSeconds.WithLabelValues("CreateTaskActivity", "success").Observe(0.2)
	observability.InstanceDegradedTotal.WithLabelValues("tenant-1").Inc()
	observability.WorkerActiveQueues.Set(1)
	observability.InternalEventsLastReceivedTimestamp.WithLabelValues("delegation.started").Set(1)
	observability.WorkflowReplayFailuresTotal.WithLabelValues("InstanceWorkflow").Inc()
	observability.InstanceDegradedCurrent.WithLabelValues("tenant-1").Set(1)
	observability.InstanceDegradedOldestAgeSeconds.WithLabelValues("tenant-1").Set(1)
	observability.WorkerQueueLastPollTimestamp.WithLabelValues("tenant-1-queue").Set(1)
	observability.SLABreachesTotal.WithLabelValues("tenant-1").Inc()
	observability.OldestReadyTaskAgeSeconds.WithLabelValues("tenant-1").Set(1)
	// RLSViolationsTotal and UpstreamDependencyErrorsTotal are skipped here —
	// both are already pre-initialized (fully closed label sets), so they
	// already appear in any post-Register scrape; incrementing them here
	// would corrupt TestRegister_PreInitializesClosedLabelSets' zero-value
	// assertion, since both tests share the process-global default registry.
	observability.WFCacheHitsTotal.Inc()
	observability.WFCacheMissesTotal.Inc()
	observability.OutboxOldestUnpublishedAgeSeconds.Set(1)

	body := scrape(t)

	metricNames := []string{
		"record_version_conflict_total",
		"db_tx_retry_total",
		"internal_events_ingest_total",
		"delegation_reroute_duration_seconds",
		"task_signal_duration_seconds",
		"instance_start_duration_seconds",
		"workflow_activity_duration_seconds",
		"instance_degraded_total",
		"worker_active_queues",
		"internal_events_last_received_timestamp",
		"rls_violations_total",
		"workflow_replay_failures_total",
		"instance_degraded_current",
		"instance_degraded_oldest_age_seconds",
		"worker_queue_last_poll_timestamp",
		"sla_breaches_total",
		"oldest_ready_task_age_seconds",
		"upstream_dependency_errors_total",
		"wf_cache_hits_total",
		"wf_cache_misses_total",
		"outbox_oldest_unpublished_age_seconds",
	}

	for _, name := range metricNames {
		require.Truef(t, strings.Contains(body, name), "expected metric %q in scrape output", name)
	}
}

func TestRegister_IsIdempotent(t *testing.T) {
	require.NotPanics(t, func() {
		observability.Register()
		observability.Register()
	})
}

func TestRegister_PreInitializesClosedLabelSets(t *testing.T) {
	body := scrape(t)

	for _, sample := range []string{
		`task_signal_duration_seconds_count{operation="claim"}`,
		`task_signal_duration_seconds_count{operation="complete"}`,
		`task_signal_duration_seconds_count{operation="defer"}`,
		`rls_violations_total{type="missing_guc"} 0`,
		`rls_violations_total{type="cross_tenant"} 0`,
		`upstream_dependency_errors_total{dependency="definition_service"} 0`,
		`upstream_dependency_errors_total{dependency="iam"} 0`,
	} {
		require.Truef(t, strings.Contains(body, sample), "expected pre-initialized series %q", sample)
	}
}
