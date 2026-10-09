package observability

import (
	"fmt"

	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

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
