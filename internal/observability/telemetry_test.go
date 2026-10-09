package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/obsregistry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestInitTelemetry_SharedCollectorsAreRegistered(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	log, err := logger.NewLogger("test")
	require.NoError(t, err)

	ginCfg := observability.GinConfig("workflow-execution-service", "test", "test", log)
	shutdown, err := observability.InitTelemetry(ginCfg, log)
	require.NoError(t, err)
	t.Cleanup(shutdown)

	require.NotNil(t, observability.RecordVersionConflictTotal)

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range gathered {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"platform_build_info",
		"platform_library_info",
		"workflow_execution_service_rls_violations_total",
		"workflow_execution_service_upstream_dependency_errors_total",
	} {
		assert.True(t, names[want], "%s must be exported", want)
	}

	rep, err := obsregistry.Validate(gincommon.MetricsGatherer(), obsregistry.Options{})
	require.NoError(t, err)
	assert.Empty(t, rep.Errors(), "scrape must conform to the Platform Observability Registry")
}

func TestTraceID_EmptyWithoutSpan(t *testing.T) {
	require.Empty(t, observability.TraceID(t.Context()))
}

func TestStartSpan_ReturnsEnd(t *testing.T) {
	ctx, end := observability.StartSpan(t.Context(), "test_span")
	require.NotNil(t, ctx)
	require.NotPanics(t, end)
}
