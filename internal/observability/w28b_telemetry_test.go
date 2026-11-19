package observability_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestRegister_ServiceCollectorNameAlreadyTaken_PanicsAndLaterCollectorsStayNil(t *testing.T) {
	if !w28bInFreshProcess(t) {
		return
	}
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		ServiceName:  "workflow-execution-service",
		BuildVersion: "test",
		Domain:       observability.Domain,
		Environment:  "test",
	})
	require.NoError(t, gincommon.MetricsRegisterer().Register(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "workflow_execution_service_tenant_purge_failures_total",
		Help: "taken by another collector",
	})))

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		observability.Register()
	}()

	msg, isString := recovered.(string)
	require.True(t, isString, "Register must panic with a string, got %#v", recovered)
	assert.True(t, strings.HasPrefix(msg, "metrics: register service collector: "), msg)
	assert.NotNil(t, observability.RecordVersionConflictTotal, "collectors built before the clash stay registered")
	assert.NotNil(t, observability.RetentionRowsDeletedTotal)
	assert.Nil(t, observability.TenantPurgeFailuresTotal, "the clashing collector is never assigned")
	assert.Nil(t, observability.ConnectorDispatchDeadLetteredTotal, "collectors after the clash are never built")
	assert.Nil(t, observability.TaskSignalDurationSeconds)
	assert.Nil(t, observability.WFCacheMissesTotal)

	require.NotPanics(t, observability.Register, "the construction ran once; a retry is a no-op")
	assert.Nil(t, observability.WFCacheMissesTotal)
}

func TestInitTelemetry_FreshProcessEmptyServiceName_FailsAsTracingAndRegistersNothing(t *testing.T) {
	if !w28bInFreshProcess(t, "OTEL_EXPORTER_OTLP_ENDPOINT=none") {
		return
	}
	log := &w28bLogger{}

	shutdown, err := observability.InitTelemetry(observability.GinConfig("", "test", "test", log), log)

	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "tracing: "), err.Error())
	require.Error(t, errUnwrapOnce(err), "the tracing cause stays wrapped")
	assert.Nil(t, shutdown)
	assert.Nil(t, observability.RecordVersionConflictTotal, "service collectors must not be built")
	assert.Nil(t, observability.RLSViolationsTotal)
	assert.Empty(t, log.at("warn"))
	assert.False(t, w28bHasPrefixedFamily(t, "workflow_execution_service_"))
	for _, name := range []string{"platform_messages_received_total", "platform_db_queries_total"} {
		require.NoErrorf(t, prometheus.DefaultRegisterer.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: "probe"})),
			"%s must still be free: neither the events nor the pgcommon metrics were registered", name)
	}
}

func errUnwrapOnce(err error) error {
	type unwrapper interface{ Unwrap() error }
	if u, ok := err.(unwrapper); ok { //nolint:errorlint // the test inspects one wrapping level on purpose
		return u.Unwrap()
	}
	return nil
}

func TestInitTelemetry_RepeatedInTheSameProcess_RegistersNoDuplicateAndWarnsOfNothing(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	first := &w28bLogger{}
	second := &w28bLogger{}

	shutdownFirst, err := observability.InitTelemetry(observability.GinConfig("workflow-execution-service", "test", "test", first), first)
	require.NoError(t, err)
	shutdownSecond, err := observability.InitTelemetry(observability.GinConfig("workflow-execution-service", "test", "test", second), second)
	require.NoError(t, err)

	assert.Empty(t, second.at("warn"), "a repeat must not warn that a metric could not be registered")
	assert.Empty(t, second.at("error"))
	families := w28bFamilies(t)
	assert.Contains(t, families, "platform_library_info", "the shared events metrics are still exported once")
	assert.Contains(t, families, "workflow_execution_service_rls_violations_total")
	require.NotNil(t, shutdownFirst)
	require.NotNil(t, shutdownSecond)
	assert.NotPanics(t, shutdownFirst)
	assert.NotPanics(t, shutdownSecond)
}

func TestInitTelemetry_FreshProcess_EveryServiceSeriesCarriesTheIdentityLabels(t *testing.T) {
	if !w28bInFreshProcess(t, "OTEL_EXPORTER_OTLP_ENDPOINT=none") {
		return
	}
	log := &w28bLogger{}

	shutdown, err := observability.InitTelemetry(observability.GinConfig("workflow-execution-service", "test", "staging", log), log)
	require.NoError(t, err)
	defer shutdown()

	checked := 0
	for name, mf := range w28bFamilies(t) {
		if !strings.HasPrefix(name, "workflow_execution_service_") {
			continue
		}
		for _, s := range w28bSamplesOf(mf) {
			checked++
			assert.Equalf(t, "workflow", s.labels["domain"], "%s", name)
			assert.Equalf(t, "workflow-execution-service", s.labels["service"], "%s", name)
			assert.Equalf(t, "staging", s.labels["environment"], "%s: the identity is fixed before Register builds the collectors", name)
		}
	}
	assert.Greater(t, checked, 20, "series checked")
}

func TestStartSpan_WithAParentSpan_StartsAChildUnderTheServiceTracerAndEndsOnce(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	t.Setenv("OTEL_TRACES_SAMPLER_RATIO", "1")
	log := &w28bLogger{}
	shutdown, err := observability.InitTelemetry(observability.GinConfig("workflow-execution-service", "test", "test", log), log)
	require.NoError(t, err)
	t.Cleanup(shutdown)
	parentCtx, parent := gincommon.NewTracer("w28b-parent").Start(t.Context(), "w28b-parent-span")
	defer parent.End()

	ctx, end := observability.StartSpan(parentCtx, "w28b-child-span")

	carried := oteltrace.SpanFromContext(ctx)
	require.True(t, carried.SpanContext().IsValid())
	assert.NotEqual(t, parent.SpanContext().SpanID(), carried.SpanContext().SpanID(), "the context carries the new span, not the parent")
	assert.Equal(t, parent.SpanContext().TraceID(), carried.SpanContext().TraceID())
	assert.Equal(t, carried.SpanContext().TraceID().String(), observability.TraceID(ctx))
	assert.True(t, carried.IsRecording())
	child, ok := carried.(sdktrace.ReadOnlySpan)
	require.True(t, ok, "the span is an SDK span")
	assert.Equal(t, "w28b-child-span", child.Name())
	assert.Equal(t, "workflow/workflow-execution-service", child.InstrumentationScope().Name)
	assert.Equal(t, parent.SpanContext().SpanID(), child.Parent().SpanID())
	assert.True(t, child.EndTime().IsZero(), "nothing ends before the returned func is called")

	end()
	endedAt := child.EndTime()
	end()

	assert.False(t, carried.IsRecording())
	assert.False(t, endedAt.IsZero())
	assert.True(t, endedAt.Equal(child.EndTime()), "a second end changes nothing")
}
