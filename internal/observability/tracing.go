package observability

import (
	"fmt"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

// InitTracing wraps gincommon.InitTracingFromEnv with a caller-side no-op
// guard. gincommon's own OTEL_EXPORTER_OTLP_ENDPOINT default (localhost:4317)
// is never empty, so without this guard it always tries constructing a real
// OTLP exporter, producing noisy connection errors in dev/test environments
// with no collector running — the same guard iam-user-profile's
// cmd/server/main.go applies before calling the same function.
func InitTracing() (shutdown func()) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func() {
			// No collector configured — nothing was initialized to shut down.
		}
	}
	return gincommon.InitTracingFromEnv()
}

// NewTemporalTracingInterceptor wraps the Temporal Go SDK's own OTel
// interceptor (execution LLD §7.6). Temporal does not auto-propagate W3C
// trace context across SignalWorkflow/StartWorkflow -> Activity without it;
// the one interceptor value this returns is usable on both
// client.Options.Interceptors (API process) and worker.Options.Interceptors
// (Worker process) — interceptor.Interceptor embeds both ClientInterceptor
// and WorkerInterceptor. Passing a zero-value opentelemetry.TracerOptions
// uses whatever tracer provider InitTracing installed globally.
func NewTemporalTracingInterceptor(opts opentelemetry.TracerOptions) (interceptor.Interceptor, error) {
	i, err := opentelemetry.NewTracingInterceptor(opts)
	if err != nil {
		return nil, fmt.Errorf("new temporal tracing interceptor: %w", err)
	}
	return i, nil
}
