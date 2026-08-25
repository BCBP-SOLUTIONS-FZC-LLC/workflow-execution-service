// cmd/connector-worker executes connector-typed tasks: it consumes the
// Valkey Stream execution_service's internal-events handler pushes
// connector-typed workflow.task.created events onto, dispatches to a real
// workflow-connectors.Connector, and reports the outcome back via
// execution_service's own /internal/connector-tasks HTTP endpoints.
//
// This binary never imports go.temporal.io/sdk — see design/LLD/
// workflow_connectors.md §6.1 Decision #2. It reaches the workflow only
// through execution_service's own HTTP completion endpoint
// (internal/adapter/inbound/http/handler/connector_task_completion.go),
// never the Temporal SDK directly.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	if err := checkRequiredConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "cmd/connector-worker: %v\n", err)
		os.Exit(1)
	}

	observability.Register()
	tracingShutdown := observability.InitTracing()
	defer tracingShutdown()

	wlog, err := logger.NewLogger(cfg.AppEnv)
	if err != nil {
		log.Fatalf("cmd/connector-worker: new logger: %v", err)
	}

	deps, cleanup, err := buildDeps(cfg)
	if err != nil {
		log.Fatalf("cmd/connector-worker: %v", err)
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go runDispatchLoop(ctx, deps, &wg, wlog)

	healthSrv := newHealthServer(fmt.Sprintf(":%d", cfg.WorkerHealthPort), deps)
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wlog.Error("health server exited", map[string]any{"error": err.Error()})
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Stop picking up new work immediately; in-flight dispatches (each
	// bounded by its own per-type pool timeout, not this cancellation) get
	// a bounded grace period to finish and report their outcome before the
	// process exits.
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		wlog.Warn("health server shutdown error", map[string]any{"error": err.Error()})
	}

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(30 * time.Second):
		wlog.Warn("connector-worker: shutdown grace period elapsed with dispatches still in flight", nil)
	}
}

func checkRequiredConfig(cfg *config.Config) error {
	switch {
	case cfg.ConnectorStreamKey == "":
		return fmt.Errorf("CONNECTOR_STREAM_KEY is required")
	case cfg.OpenBaoAddr == "":
		return fmt.Errorf("OPENBAO_ADDR is required")
	case cfg.DefinitionServiceInternalHTTPAddr == "":
		return fmt.Errorf("DEFINITION_SERVICE_INTERNAL_HTTP_ADDR is required")
	case cfg.ExecutionServiceInternalAddr == "":
		return fmt.Errorf("EXECUTION_SERVICE_INTERNAL_ADDR is required")
	case cfg.AppEnv == "prod" && cfg.InternalAPIToken == "":
		return fmt.Errorf("INTERNAL_API_TOKEN is required when APP_ENV=prod")
	}
	return nil
}
