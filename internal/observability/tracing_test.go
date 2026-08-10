package observability_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	commonpb "go.temporal.io/api/common/v1"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestInitTracing_NoOpWhenEndpointUnset(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	before := otel.GetTracerProvider()
	shutdown := observability.InitTracing()

	require.NotNil(t, shutdown)
	require.NotPanics(t, shutdown)
	require.Equal(t, before, otel.GetTracerProvider(), "no-op guard must not touch the global tracer provider")
}

func TestInitTracing_InitializesWhenEndpointSet(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317")
	t.Setenv("OTEL_SERVICE_NAME", "execution-service-test")

	shutdown := observability.InitTracing()
	t.Cleanup(shutdown)

	require.NotNil(t, shutdown)
	_, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	require.True(t, ok, "expected a real *sdktrace.TracerProvider installed globally once an endpoint is configured")
}

// activityUnderTest is a trivial Activity used only to prove a span reaches
// it; its body does nothing observable itself.
func activityUnderTest(_ context.Context) error {
	return nil
}

func workflowUnderTest(ctx workflow.Context) error {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}
	ctx = workflow.WithActivityOptions(ctx, ao)
	return workflow.ExecuteActivity(ctx, activityUnderTest).Get(ctx, nil)
}

func TestNewTemporalTracingInterceptor_PropagatesTraceIntoActivity(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer, err := opentelemetry.NewTracer(opentelemetry.TracerOptions{
		Tracer:    tp.Tracer("test"),
		HeaderKey: "test-tracer-data",
	})
	require.NoError(t, err)

	// Simulate a span started in an inbound HTTP handler, the way a real
	// client.Options interceptor would see one on the context passed into
	// SignalWorkflow/StartWorkflow.
	handlerCtx, handlerSpan := tp.Tracer("test").Start(context.Background(), "http.handler")
	defer handlerSpan.End()
	clientSpan := tracer.SpanFromContext(handlerCtx)
	require.NotNil(t, clientSpan)

	// Marshal it into the same map+payload shape the SDK's own client-side
	// interceptor writes into the outbound Temporal header (mirrors
	// interceptor.tracingInterceptor.writeSpanToHeader in the SDK).
	marshaled, err := tracer.MarshalSpan(clientSpan)
	require.NoError(t, err)
	payload, err := converter.GetDefaultDataConverter().ToPayload(marshaled)
	require.NoError(t, err)

	interceptorUnderTest, err := observability.NewTemporalTracingInterceptor(opentelemetry.TracerOptions{
		Tracer:    tp.Tracer("test"),
		HeaderKey: "test-tracer-data",
	})
	require.NoError(t, err)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(workflowUnderTest)
	env.RegisterActivity(activityUnderTest)
	env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{"test-tracer-data": payload}})
	env.SetWorkerOptions(worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{interceptorUnderTest},
	})

	env.ExecuteWorkflow(workflowUnderTest)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	wantTraceID := handlerSpan.SpanContext().TraceID()

	var sawActivitySpan bool
	for _, s := range recorder.Ended() {
		if s.SpanContext().TraceID() != wantTraceID {
			continue
		}
		if strings.Contains(s.Name(), "RunActivity") {
			sawActivitySpan = true
		}
	}
	require.True(t, sawActivitySpan,
		"expected an Activity-side span sharing the HTTP handler's trace ID — trace propagation broke somewhere between the header and the Activity")
}
