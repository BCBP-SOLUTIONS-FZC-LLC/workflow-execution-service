package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/obsregistry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestStandard_LibrariesShareRegistryCollectors(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	ginCfg := observability.GinConfig("workflow-execution-service", "test", "test", log)
	shutdown, err := observability.InitTelemetry(ginCfg, log)
	require.NoError(t, err)
	t.Cleanup(shutdown)

	require.NotNil(t, observability.RecordVersionConflictTotal, "Register must build the service collectors once the identity is set")

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	for _, mw := range gincommon.ObservabilityMiddlewares(ginCfg) {
		engine.Use(mw)
	}
	engine.GET("/healthz", gincommon.HealthHandler())
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	exerciseEveryCollector()

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range gathered {
		names[mf.GetName()] = true
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "version" && strings.HasPrefix(mf.GetName(), "platform_") {
					assert.Equal(t, "platform_build_info", mf.GetName(), "version label only on platform_build_info")
				}
			}
		}
	}
	for _, want := range []string{
		"platform_build_info",
		"platform_library_info",
		"platform_http_requests_total",
		"platform_http_request_duration_seconds",
		"workflow_execution_service_record_version_conflict_total",
		"workflow_execution_service_task_signal_duration_seconds",
		"workflow_execution_service_instance_degraded_current",
		"workflow_execution_service_rls_violations_total",
	} {
		assert.True(t, names[want], "%s must be exported", want)
	}

	for _, rules := range []string{"../../deploy/monitoring/app-alerts.yml", "../../deploy/helm/templates/prometheusrule.yaml"} {
		body, readErr := os.ReadFile(rules)
		require.NoError(t, readErr)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.Contains(line, "name: workflow_execution_service_") {
				continue
			}
			for _, token := range ruleMetricPattern.FindAllString(line, -1) {
				name := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(token, "_bucket"), "_count"), "_sum")
				assert.True(t, names[name], "%s references %s, which the service does not export", rules, token)
			}
		}
	}

	rep, err := obsregistry.Validate(gincommon.MetricsGatherer(), obsregistry.Options{})
	require.NoError(t, err)
	assert.Empty(t, rep.Errors(), "scrape must conform to the Platform Observability Registry")

	if out := os.Getenv("METRICS_SCRAPE_OUT"); out != "" {
		scrape := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
		req.Header.Set("Accept", "text/plain")
		gincommon.MetricsHandler().ServeHTTP(scrape, req)
		require.NoError(t, os.WriteFile(out, scrape.Body.Bytes(), 0o600))
	}
}

var ruleMetricPattern = regexp.MustCompile(`workflow_execution_service_[a-z_]+`)

func exerciseEveryCollector() {
	observability.IncCounterVec(observability.RecordVersionConflictTotal, "workflow_instance")
	observability.IncCounterVec(observability.InternalEventsIngestTotal, "DelegationStarted", "ok")
	observability.IncCounterVec(observability.DelegationScopeUnhandledTotal, "some-new-scope")
	observability.AddRetentionRowsDeleted("outbox_events", 1)
	observability.IncCounterVec(observability.ConnectorDispatchDeadLetteredTotal, "send-email")
	observability.ObserveHistogram(observability.DelegationRerouteDurationSeconds, 1)
	observability.ObserveHistogramVec(observability.TaskSignalDurationSeconds, 0.1, "claim")
	observability.ObserveHistogram(observability.InstanceStartDurationSeconds, 0.5)
	observability.ObserveHistogramVec(observability.WorkflowActivityDurationSeconds, 0.2, "CreateTaskActivity", "success")
	observability.IncCounter(observability.InstanceDegradedTotal)
	observability.SetGauge(observability.WorkerActiveQueues, 1)
	observability.InternalEventsLastReceivedTimestamp.WithLabelValues("DelegationStarted").Set(1)
	observability.AddRLSViolations("cross_tenant_access", 1)
	observability.IncCounterVec(observability.WorkflowReplayFailuresTotal, "InstanceWorkflow")
	observability.RecordOperationalStats(1, 1, 1)
	observability.WorkerQueueLastPollTimestamp.WithLabelValues("default-queue").Set(1)
	observability.IncCounter(observability.SLABreachesTotal)
	observability.IncCounterVec(observability.UpstreamDependencyErrorsTotal, "iam")
	observability.IncCounter(observability.WFCacheHitsTotal)
	observability.IncCounter(observability.WFCacheMissesTotal)
}
