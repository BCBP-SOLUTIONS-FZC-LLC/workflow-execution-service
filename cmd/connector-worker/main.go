// cmd/connector-worker executes connector-typed tasks: it consumes the
// Valkey Stream execution_service's internal-events handler pushes
// connector-typed workflow.task.created events onto, dispatches to a real
// workflow-connectors.Connector, and reports the outcome back via
// execution_service's own /api/v1/internal/connector-tasks HTTP endpoints.
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

// version is stamped at build time by the Dockerfile's
// -ldflags "-X main.version=${BUILD_VERSION}".
var version = "dev"

func main() {
	if config.VersionRequested() {
		fmt.Println(version)
		return
	}

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
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	var wg sync.WaitGroup
	go heartbeatLoop(heartbeatCtx, deps, wlog)
	loopsDone := make(chan struct{})
	go func() {
		runDispatchLoop(ctx, deps, &wg, wlog)
		close(loopsDone)
	}()
	go runAliasRefreshLoop(ctx, deps, cfg, wlog)

	healthSrv := newHealthServer(fmt.Sprintf(":%d", cfg.WorkerHealthPort), deps)
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wlog.Error("health server exited", map[string]any{"error": err.Error()})
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Stop picking up new work. A started run is detached from this
	// cancellation, so the drain waits for the slowest one to report, and the
	// heartbeat keeps its entry from being reclaimed meanwhile.
	cancel()

	drained := make(chan struct{})
	go func() {
		<-loopsDone
		wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(shutdownDrain(cfg)):
		wlog.Warn("connector-worker: shutdown drain elapsed with dispatches still in flight", nil)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		wlog.Warn("health server shutdown error", map[string]any{"error": err.Error()})
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
	case cfg.ConnectorActorUserIDs == "":
		return fmt.Errorf("CONNECTOR_ACTOR_USER_IDS is required")
	case cfg.AppEnv == "prod" && cfg.InternalAPIToken == "":
		return fmt.Errorf("INTERNAL_API_TOKEN is required when APP_ENV=prod")
	case cfg.ConnectorStreamMaxDeliveries < 1:
		return fmt.Errorf("CONNECTOR_STREAM_MAX_DELIVERIES must be >= 1")
	case cfg.ConnectorStreamDeadLetterKey == "" || cfg.ConnectorStreamDeadLetterKey == cfg.ConnectorStreamKey:
		return fmt.Errorf("CONNECTOR_STREAM_DEAD_LETTER_KEY is required and must differ from CONNECTOR_STREAM_KEY")
	case cfg.ConnectorStreamClaimMinIdle < 3*time.Second:
		return fmt.Errorf("CONNECTOR_STREAM_CLAIM_MIN_IDLE must be at least 3s, the heartbeat runs three times within it")
	// Reclaim runs every claim-min-idle, so redeliveries land up to twice that apart.
	case 2*time.Duration(cfg.ConnectorStreamMaxDeliveries)*cfg.ConnectorStreamClaimMinIdle >= outcomeTTL:
		return fmt.Errorf("2 x CONNECTOR_STREAM_MAX_DELIVERIES x CONNECTOR_STREAM_CLAIM_MIN_IDLE must stay under the %s outcome record", outcomeTTL)
	}
	return nil
}
