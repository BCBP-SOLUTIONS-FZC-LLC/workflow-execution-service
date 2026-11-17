package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/pglogger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

var version = "dev"

func main() {
	if config.VersionRequested() {
		fmt.Println(version)
		return
	}

	cfg, err := config.Load()
	if err == nil {
		err = cfg.RequireWorker()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	wlog, err := logger.NewLogger(cfg.AppEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cmd/worker: new logger: %v\n", err)
		os.Exit(1)
	}
	tracingShutdown, err := observability.InitTelemetry(observability.GinConfig(cfg.OTELServiceName, cfg.BuildVersion, cfg.AppEnv, wlog), wlog)
	if err != nil {
		wlog.Error("telemetry init failed", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	defer tracingShutdown()

	bound, err := bindListeners(context.Background(), fmt.Sprintf(":%d", cfg.WorkerHealthPort), fmt.Sprintf(":%d", cfg.WorkerMetricsPort))
	if err != nil {
		fatal(wlog, tracingShutdown, "startup failed", err)
	}
	defer bound.close()

	deps, queueRepo, pool, cleanup, err := buildDeps(cfg, wlog)
	if err != nil {
		fatal(wlog, tracingShutdown, "startup failed", err)
	}
	defer cleanup()

	dialOptions, err := temporalDialOptions(cfg)
	if err != nil {
		fatal(wlog, func() { cleanup(); tracingShutdown() }, "temporal client options", err)
	}
	sdk, err := client.Dial(dialOptions)
	if err != nil {
		fatal(wlog, func() { cleanup(); tracingShutdown() }, "dial temporal", err)
	}
	defer sdk.Close()

	if cfg.WorkflowCacheSize > 0 {
		worker.SetStickyWorkflowCacheSize(cfg.WorkflowCacheSize)
	}

	registry := newWorkerRegistry()
	defaultWorker, err := startWorkerForQueue(sdk, deps, cfg.TemporalTaskQueue, worker.Options{WorkerStopTimeout: 25 * time.Second, Interceptors: []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()}})
	if err != nil {
		fatal(wlog, func() { cleanup(); tracingShutdown() }, "start default worker", err)
	}
	registry.add(cfg.TemporalTaskQueue, defaultWorker)

	pollCtx, cancelPoll := context.WithCancel(context.Background())
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		pollQueueTopology(pollCtx, sdk, deps, queueRepo, cfg.TemporalTaskQueue, cfg.QueueTopologyPollInterval, registry, wlog)
	}()

	healthSrv := newHealthServer(bound.health.Addr().String(), pool, sdk)
	serveOn("health", healthSrv, bound.health, wlog)

	metricsSrv := newMetricsServer(bound.metrics.Addr().String())
	serveOn("metrics", metricsSrv, bound.metrics, wlog)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancelPoll()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		wlog.Warn("health server shutdown error", map[string]any{"error": err.Error()})
	}
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		wlog.Warn("metrics server shutdown error", map[string]any{"error": err.Error()})
	}

	stopTopology(cancelPoll, pollDone, registry)
}

func buildDeps(cfg *config.Config, wlog port.Logger) (*outboundtemporal.Deps, port.ActiveTaskQueueRepository, *pgcommon.Pool, func(), error) {
	pool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{
		DSN:                cfg.DatabaseURL,
		MaxConns:           cfg.PGMaxConns,
		MinConns:           cfg.PGMinConns,
		PGBouncerMode:      cfg.PGBouncerMode,
		SlowQueryThreshold: time.Duration(cfg.PGSlowQueryThresholdMS) * time.Millisecond,
		GUCProvider:        pgcommon.GUCSetFromContext,
		Logger:             pglogger.New(wlog),
		Tracer:             gincommon.NewSpanTracer(cfg.OTELServiceName),
	})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("new postgres pool: %w", err)
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		pool.Close()
		return nil, nil, nil, nil, fmt.Errorf("new schema validator: %w", err)
	}

	definitions := outboundhttp.NewDefinitionClient(cfg.DefinitionServiceURL, cfg.DefinitionClientTimeout)

	deps := &outboundtemporal.Deps{
		Instances:   postgres.NewInstanceRepo(pool),
		Tasks:       postgres.NewTaskRepo(pool),
		Assignments: postgres.NewTaskAssignmentRepo(pool),
		Outbox:      postgres.NewOutboxRepo(pool),
		Transactor:  postgres.NewTransactor(pool),
		Validator:   validator,
		Definitions: definitions,
	}
	queueRepo := postgres.NewActiveTaskQueueRepo(pool)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = pool.DrainAndClose(ctx)
	}
	return deps, queueRepo, pool, cleanup, nil
}

func fatal(wlog port.Logger, flush func(), msg string, err error) {
	wlog.Error(msg, map[string]any{"error": err.Error()})
	flush()
	os.Exit(1)
}
