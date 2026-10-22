package observability

import (
	"fmt"

	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

func NewTemporalTracingInterceptor(opts opentelemetry.TracerOptions) (interceptor.Interceptor, error) {
	i, err := opentelemetry.NewTracingInterceptor(opts)
	if err != nil {
		return nil, fmt.Errorf("new temporal tracing interceptor: %w", err)
	}
	return i, nil
}
