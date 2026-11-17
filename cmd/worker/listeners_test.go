package main

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

func busyPort(t *testing.T) (string, net.Listener) {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port), ln
}

func TestBindListeners_BothPortsFreeAreBoundAndHeld(t *testing.T) {
	lns, err := bindListeners(context.Background(), ":0", ":0")
	if err != nil {
		t.Fatalf("bindListeners() error = %v", err)
	}
	defer lns.close()

	if lns.health == nil || lns.metrics == nil {
		t.Fatalf("listeners = %+v, want both set", lns)
	}
	if _, err := net.Listen("tcp", lns.health.Addr().String()); err == nil {
		t.Error("the health port can be bound again: the listener is not held")
	}
}

func TestBindListeners_ABusyHealthPortFailsTheStartNamingIt(t *testing.T) {
	port, _ := busyPort(t)

	lns, err := bindListeners(context.Background(), ":"+port, ":0")

	if err == nil || lns != nil {
		t.Fatalf("bindListeners() = %v, %v, want an error and no listeners", lns, err)
	}
	if !strings.Contains(err.Error(), "WORKER_HEALTH_PORT") || !strings.Contains(err.Error(), port) {
		t.Errorf("error = %v, want one naming WORKER_HEALTH_PORT and the port %s", err, port)
	}
}

func TestBindListeners_ABusyMetricsPortFailsTheStartAndReleasesTheHealthPort(t *testing.T) {
	port, _ := busyPort(t)
	healthProbe, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	healthAddr := healthProbe.Addr().String()
	_ = healthProbe.Close()

	lns, err := bindListeners(context.Background(), healthAddr, ":"+port)

	if err == nil || lns != nil {
		t.Fatalf("bindListeners() = %v, %v, want an error and no listeners", lns, err)
	}
	if !strings.Contains(err.Error(), "WORKER_METRICS_PORT") || !strings.Contains(err.Error(), port) {
		t.Errorf("error = %v, want one naming WORKER_METRICS_PORT and the port %s", err, port)
	}
	again, err := net.Listen("tcp", healthAddr)
	if err != nil {
		t.Fatalf("the health port stayed bound after the failed start: %v", err)
	}
	_ = again.Close()
}

func TestWorkerBinary_APortThatCannotBeBoundFailsTheStartBeforeAnyDependencyIsTouched(t *testing.T) {
	bin := wkbootWorkerBinary(t)
	port, _ := busyPort(t)

	res := wkbootRunWorker(t, bin, []string{
		"DATABASE_URL=wkboot-not-a-dsn",
		"DEFINITION_SERVICE_URL=http://definition.invalid",
		"WORKER_HEALTH_PORT=" + port,
	})

	if res.exitCode != 1 {
		t.Errorf("exit code = %d, want 1", res.exitCode)
	}
	if !strings.Contains(res.stderr, "WORKER_HEALTH_PORT") {
		t.Errorf("stderr does not name the port that could not be bound:\n%s", res.stderr)
	}
	for _, later := range []string{"new postgres pool", "dial temporal"} {
		if strings.Contains(res.stderr, later) {
			t.Errorf("stderr mentions %q: the start went on after the bind failed:\n%s", later, res.stderr)
		}
	}
}

func TestTemporalDialOptions_FollowTheResolvedTraceExportAndCarryTheCertificate(t *testing.T) {
	on := &config.Config{TemporalHostPort: "temporal:7233", TemporalNamespace: "workflow", OTELExporterEndpoint: "collector:4317"}
	opts, err := temporalDialOptions(on)
	if err != nil {
		t.Fatalf("temporalDialOptions() error = %v", err)
	}
	if opts.HostPort != "temporal:7233" || opts.Namespace != "workflow" || len(opts.Interceptors) != 1 {
		t.Errorf("options = %+v, want the address, the namespace and one tracing interceptor", opts)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	off := &config.Config{TemporalHostPort: "temporal:7233", TemporalNamespace: "workflow", OTELExporterEndpoint: "none"}
	opts, err = temporalDialOptions(off)
	if err != nil {
		t.Fatalf("temporalDialOptions() error = %v", err)
	}
	if len(opts.Interceptors) != 0 {
		t.Errorf("interceptors = %d with export off, want none", len(opts.Interceptors))
	}

	broken := &config.Config{TemporalHostPort: "temporal:7233", TemporalNamespace: "workflow", TemporalTLSCertPath: "/nonexistent/tls.crt", TemporalTLSKeyPath: "/nonexistent/tls.key"}
	if _, err := temporalDialOptions(broken); err == nil || !strings.Contains(err.Error(), "temporal client certificate") {
		t.Errorf("error = %v, want a refusal naming the temporal client certificate", err)
	}
}
