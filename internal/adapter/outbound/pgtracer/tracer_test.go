package pgtracer_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/pgtracer"
)

func TestTracer_StartSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	tr := pgtracer.New("test-service")
	ctx, end := tr.StartSpan(context.Background(), "db.query")
	require.NotNil(t, ctx)
	require.NotNil(t, end)

	require.Empty(t, recorder.Ended(), "span must not be recorded as ended before end() is called")
	end()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "db.query", ended[0].Name())
}
