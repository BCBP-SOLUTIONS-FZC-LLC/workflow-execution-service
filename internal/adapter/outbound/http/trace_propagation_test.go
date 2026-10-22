package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

func tracedContext(t *testing.T) context.Context {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	shutdown, err := gincommon.InitTracingWithConfig(gincommon.TracingConfig{
		ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test",
	})
	if err != nil {
		t.Fatalf("init tracing: %v", err)
	}
	t.Cleanup(shutdown)
	ctx, span := gincommon.NewTracer("test").Start(context.Background(), "caller")
	t.Cleanup(func() { span.End() })
	return ctx
}

func captureTraceparent(got *string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Header.Get("Traceparent")
		w.WriteHeader(http.StatusOK)
	}))
}

func TestDefinitionClient_PropagatesTraceContext(t *testing.T) {
	var traceparent string
	srv := captureTraceparent(&traceparent)
	defer srv.Close()

	c := NewDefinitionClient(srv.URL, "token", time.Second)
	_, _, _ = c.tryGet(tracedContext(t), srv.URL+"/x")

	if traceparent == "" {
		t.Error("traceparent header missing on the Definition Service call")
	}
}

func TestEligibilityClient_PropagatesTraceContext(t *testing.T) {
	var traceparent string
	srv := captureTraceparent(&traceparent)
	defer srv.Close()

	c := NewEligibilityClient(srv.URL, time.Second)
	_, _, _ = c.tryCheckEligibility(tracedContext(t), srv.URL, uuid.New())

	if traceparent == "" {
		t.Error("traceparent header missing on the eligibility call")
	}
}

func TestAutomationPrincipalClient_PropagatesTraceContext(t *testing.T) {
	var traceparent string
	srv := captureTraceparent(&traceparent)
	defer srv.Close()

	c := NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)
	_, _ = c.AutomationPrincipal(tracedContext(t), uuid.New())

	if traceparent == "" {
		t.Error("traceparent header missing on the token service call")
	}
}
