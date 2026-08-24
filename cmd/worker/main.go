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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	if cfg.DefinitionServiceAddr == "" {
		fmt.Fprintln(os.Stderr, "DEFINITION_SERVICE_ADDR is required for cmd/worker")
		os.Exit(1)
	}

	observability.Register()
	tracingShutdown := observability.InitTracing()
	defer tracingShutdown()

	deps, queueRepo, pool, wlog, cleanup, err := buildDeps(cfg)
	if err != nil {
		log.Fatalf("cmd/worker: %v", err)
	}
	defer cleanup()

	sdk, err := client.Dial(client.Options{HostPort: cfg.TemporalHostPort, Namespace: cfg.TemporalNamespace})
	if err != nil {
		log.Fatalf("cmd/worker: dial temporal: %v", err)
	}
	defer sdk.Close()

	registry := newWorkerRegistry()
	defaultWorker, err := startWorkerForQueue(sdk, deps, cfg.TemporalTaskQueue, worker.Options{WorkerStopTimeout: 25 * time.Second})
	if err != nil {
		log.Fatalf("cmd/worker: start default worker: %v", err)
	}
	registry.add(cfg.TemporalTaskQueue, defaultWorker)

	pollCtx, cancelPoll := context.WithCancel(context.Background())
	go pollQueueTopology(pollCtx, sdk, deps, queueRepo, cfg.TemporalTaskQueue, cfg.QueueTopologyPollInterval, registry, wlog)

	healthSrv := newHealthServer(fmt.Sprintf(":%d", cfg.WorkerHealthPort), pool, sdk)
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wlog.Error("health server exited", map[string]any{"error": err.Error()})
		}
	}()

	metricsSrv := newMetricsServer(fmt.Sprintf(":%d", cfg.WorkerMetricsPort))
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wlog.Error("metrics server exited", map[string]any{"error": err.Error()})
		}
	}()

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

	var wg sync.WaitGroup
	for _, w := range registry.all() {
		wg.Add(1)
		go func(w worker.Worker) {
			defer wg.Done()
			w.Stop()
		}(w)
	}
	wg.Wait()
}

func buildDeps(cfg *config.Config) (*outboundtemporal.Deps, port.ActiveTaskQueueRepository, *pgcommon.Pool, port.Logger, func(), error) {
	wlog, err := logger.NewLogger(cfg.AppEnv)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new logger: %w", err)
	}

	pool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{
		DSN:                cfg.DatabaseURL,
		MaxConns:           cfg.PGMaxConns,
		MinConns:           cfg.PGMinConns,
		PGBouncerMode:      cfg.PGBouncerMode,
		SlowQueryThreshold: time.Duration(cfg.PGSlowQueryThresholdMS) * time.Millisecond,
		GUCProvider:        pgcommon.GUCSetFromContext,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new postgres pool: %w", err)
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		pool.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("new schema validator: %w", err)
	}

	definitions, err := outboundgrpc.NewDefinitionClient(cfg.DefinitionServiceAddr, cfg.DefinitionClientTimeout)
	if err != nil {
		pool.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("new definition client: %w", err)
	}

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
		_ = definitions.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = pool.DrainAndClose(ctx)
	}
	return deps, queueRepo, pool, wlog, cleanup, nil
}
