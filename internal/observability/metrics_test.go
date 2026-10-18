package observability_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func initIdentity(t *testing.T) {
	t.Helper()
	log, err := logger.NewLogger("dev")
	require.NoError(t, err)
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		Logger:       log,
		ServiceName:  "workflow-execution-service",
		BuildVersion: "test",
		Domain:       observability.Domain,
		Environment:  "test",
	})
	observability.Register()
}

// scrape registers metrics and returns the default registry's /metrics body.
func scrape(t *testing.T) string {
	t.Helper()

	initIdentity(t)

	srv := httptest.NewServer(gincommon.MetricsHandler())
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
	initIdentity(t)

	observability.RecordVersionConflictTotal.WithLabelValues("workflow_instance").Inc()
	observability.DelegationScopeUnhandledTotal.WithLabelValues("some-new-scope").Inc()
	observability.AddRetentionRowsDeleted("outbox_events", 1)
	observability.IncCounter(observability.TenantPurgeFailuresTotal)
	observability.InternalEventsIngestTotal.WithLabelValues("DelegationStarted", "ok").Inc()
	observability.DelegationRerouteDurationSeconds.Observe(1.5)
	observability.TaskSignalDurationSeconds.WithLabelValues("claim").Observe(0.1)
	observability.InstanceStartDurationSeconds.Observe(0.5)
	observability.WorkflowActivityDurationSeconds.WithLabelValues("CreateTaskActivity", "success").Observe(0.2)
	observability.InstanceDegradedTotal.Inc()
	observability.WorkerActiveQueues.Set(1)
	observability.InternalEventsLastReceivedTimestamp.WithLabelValues("DelegationStarted").Set(1)
	observability.WorkflowReplayFailuresTotal.WithLabelValues("InstanceWorkflow").Inc()
	observability.InstanceDegradedCurrent.Set(1)
	observability.InstanceDegradedOldestAgeSeconds.Set(1)
	observability.WorkerQueueLastPollTimestamp.WithLabelValues("tenant-1-queue").Set(1)
	observability.SLABreachesTotal.Inc()
	observability.OldestReadyTaskAgeSeconds.Set(1)
	// RLSViolationsTotal and UpstreamDependencyErrorsTotal are skipped here —
	// both are already pre-initialized (fully closed label sets), so they
	// already appear in any post-Register scrape; incrementing them here
	// would corrupt TestRegister_PreInitializesClosedLabelSets' zero-value
	// assertion, since both tests share the process-global default registry.
	observability.WFCacheHitsTotal.Inc()
	observability.WFCacheMissesTotal.Inc()

	body := scrape(t)

	metricNames := []string{
		"workflow_execution_service_record_version_conflict_total",
		"workflow_execution_service_internal_events_ingest_total",
		"workflow_execution_service_delegation_reroute_duration_seconds",
		"workflow_execution_service_task_signal_duration_seconds",
		"workflow_execution_service_instance_start_duration_seconds",
		"workflow_execution_service_workflow_activity_duration_seconds",
		"workflow_execution_service_instance_degraded_total",
		"workflow_execution_service_worker_active_queues",
		"workflow_execution_service_internal_events_last_received_timestamp",
		"workflow_execution_service_rls_violations_total",
		"workflow_execution_service_workflow_replay_failures_total",
		"workflow_execution_service_instance_degraded_current",
		"workflow_execution_service_instance_degraded_oldest_age_seconds",
		"workflow_execution_service_worker_queue_last_poll_timestamp",
		"workflow_execution_service_sla_breaches_total",
		"workflow_execution_service_oldest_ready_task_age_seconds",
		"workflow_execution_service_upstream_dependency_errors_total",
		"workflow_execution_service_wf_cache_hits_total",
		"workflow_execution_service_wf_cache_misses_total",
	}

	for _, name := range metricNames {
		require.Truef(t, strings.Contains(body, name), "expected metric %q in scrape output", name)
	}
}

func TestRegister_IsIdempotent(t *testing.T) {
	initIdentity(t)
	require.NotPanics(t, func() {
		observability.Register()
		observability.Register()
	})
}

func TestRegister_PreInitializesClosedLabelSets(t *testing.T) {
	initIdentity(t)

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)

	series := map[string]map[string]bool{}
	for _, mf := range gathered {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if series[mf.GetName()] == nil {
					series[mf.GetName()] = map[string]bool{}
				}
				series[mf.GetName()][lp.GetName()+"="+lp.GetValue()] = true
			}
		}
	}

	for name, label := range map[string][]string{
		"workflow_execution_service_task_signal_duration_seconds":     {"operation=claim", "operation=complete", "operation=defer"},
		"workflow_execution_service_rls_violations_total":             {"violation_type=missing_or_invalid_guc", "violation_type=cross_tenant_access"},
		"workflow_execution_service_upstream_dependency_errors_total": {"dependency=definition_service", "dependency=iam"},
	} {
		for _, l := range label {
			require.Truef(t, series[name][l], "expected pre-initialized series %s{%s}", name, l)
		}
	}
}

func TestRecordOperationalStats_SetsTheThreeGauges(t *testing.T) {
	initIdentity(t)

	observability.RecordOperationalStats(4, 120.5, 7)

	body := scrape(t)
	for _, want := range []string{
		"workflow_execution_service_instance_degraded_current",
		"workflow_execution_service_instance_degraded_oldest_age_seconds",
		"workflow_execution_service_oldest_ready_task_age_seconds",
	} {
		require.Contains(t, body, want)
	}
	require.Equal(t, 4.0, testutil.ToFloat64(observability.InstanceDegradedCurrent))
	require.Equal(t, 120.5, testutil.ToFloat64(observability.InstanceDegradedOldestAgeSeconds))
	require.Equal(t, 7.0, testutil.ToFloat64(observability.OldestReadyTaskAgeSeconds))
}

func TestAddRLSViolations_AddsByType(t *testing.T) {
	initIdentity(t)
	before := testutil.ToFloat64(observability.RLSViolationsTotal.WithLabelValues("cross_tenant_access"))

	observability.AddRLSViolations("cross_tenant_access", 3)

	require.Equal(t, before+3, testutil.ToFloat64(observability.RLSViolationsTotal.WithLabelValues("cross_tenant_access")))
}

func TestRecordingHelpers_IgnoreUnregisteredCollectors(t *testing.T) {
	require.NotPanics(t, func() {
		observability.IncCounter(nil)
		observability.IncCounterVec(nil, "x")
		observability.ObserveHistogram(nil, 1)
		observability.ObserveHistogramVec(nil, 1, "x")
		observability.SetGauge(nil, 1)
	})
}

func TestRecordingHelpers_RecordOnRegisteredCollectors(t *testing.T) {
	initIdentity(t)
	hits := testutil.ToFloat64(observability.WFCacheHitsTotal)
	observability.IncCounter(observability.WFCacheHitsTotal)
	require.Equal(t, hits+1, testutil.ToFloat64(observability.WFCacheHitsTotal))

	require.NotPanics(t, func() {
		observability.ObserveHistogram(observability.InstanceStartDurationSeconds, 0.2)
		observability.ObserveHistogramVec(observability.TaskSignalDurationSeconds, 0.2, "claim")
		observability.IncCounterVec(observability.UpstreamDependencyErrorsTotal, "iam")
	})
}
