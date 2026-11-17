package observability_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

type exportLog struct {
	mu        sync.Mutex
	exporters []string
}

func (l *exportLog) record(msg string, fields map[string]interface{}) {
	if msg != "otel_tracing_initialized" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.exporters = append(l.exporters, fields["exporter"].(string))
}

func (l *exportLog) Debug(msg string, f map[string]interface{}) { l.record(msg, f) }
func (l *exportLog) Info(msg string, f map[string]interface{})  { l.record(msg, f) }
func (l *exportLog) Warn(msg string, f map[string]interface{})  { l.record(msg, f) }
func (l *exportLog) Error(msg string, f map[string]interface{}) { l.record(msg, f) }

func initWith(t *testing.T, endpoint, exporter string) []string {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
	t.Setenv("OTEL_TRACES_EXPORTER", exporter)
	log := &exportLog{}

	shutdown, err := observability.InitTelemetry(observability.GinConfig("workflow-execution-service", "test", "test", log), log)

	require.NoError(t, err)
	t.Cleanup(shutdown)
	return log.exporters
}

func TestInitTelemetry_AnUnsetEndpointDisablesTraceExport(t *testing.T) {
	exporters := initWith(t, "", "")

	require.NotEmpty(t, exporters)
	for _, state := range exporters {
		assert.Contains(t, state, "disabled")
	}
}

func TestInitTelemetry_AConfiguredEndpointStillExports(t *testing.T) {
	exporters := initWith(t, "127.0.0.1:1", "")

	require.NotEmpty(t, exporters)
	for _, state := range exporters {
		assert.Equal(t, "otlp-grpc", state)
	}
}

func TestTracingClientInterceptors_FollowTheResolvedExportSetting(t *testing.T) {
	off, err := observability.TracingClientInterceptors(false)
	require.NoError(t, err)
	assert.Empty(t, off)

	on, err := observability.TracingClientInterceptors(true)
	require.NoError(t, err)
	assert.Len(t, on, 1)
}
