package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type app struct {
	cfg *config.Config
	log port.Logger

	appPool   *pgcommon.Pool
	inboxPool *pgcommon.Pool
	relayPool *pgcommon.Pool
	lockPool  *pgcommon.Pool
	cache     io.Closer

	httpServer    *http.Server
	metricsServer *http.Server
	outboxRelay   *outbox.Runner

	rlsExporter      *service.RLSViolationExporter
	statsSampler     *service.OperationalGaugeSampler
	retentionSweeper *service.RetentionSweeper
	tenantPurger     *service.TenantPurger
	cancelExporters  context.CancelFunc
	loops            sync.WaitGroup

	sdkClient client.Client

	tracingShutdown func()

	shuttingDown *atomic.Bool
	sleep        func(time.Duration)
}

func (a *app) run() error {
	serveErrs := a.startServers()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	stopCause := a.awaitStop(quit, serveErrs)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	a.stopServers(shutdownCtx)

	a.log.Info("server stopped", nil)
	return stopCause
}

func (a *app) awaitStop(quit <-chan os.Signal, serveErrs <-chan error) error {
	select {
	case sig := <-quit:
		a.log.Info("shutting down", map[string]any{"signal": sig.String()})
		a.drain()
		return nil
	case err := <-serveErrs:
		a.log.Error("a listener failed; shutting down", map[string]any{"error": err.Error()})
		return err
	}
}

func (a *app) drain() {
	a.shuttingDown.Store(true)
	if a.cfg.ShutdownDrainDelay > 0 {
		a.sleep(a.cfg.ShutdownDrainDelay)
	}
}

func (a *app) listen(name string, srv *http.Server, serveErrs chan<- error) {
	go func() {
		a.log.Info(name+" server starting", map[string]any{"addr": srv.Addr})
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrs <- fmt.Errorf("%s server: %w", name, err)
		}
	}()
}

func (a *app) startServers() <-chan error {
	exportCtx, cancelExporters := context.WithCancel(context.Background())
	a.cancelExporters = cancelExporters
	a.goLoop(exportCtx, a.rlsExporter.Run)
	a.goLoop(exportCtx, a.statsSampler.Run)
	a.goLoop(exportCtx, a.retentionSweeper.Run)
	if a.tenantPurger != nil {
		a.goLoop(exportCtx, a.tenantPurger.Run)
	}

	serveErrs := make(chan error, 2)
	a.listen("HTTP", a.httpServer, serveErrs)
	a.listen("metrics", a.metricsServer, serveErrs)

	go a.runRelay(a.outboxRelay.Start, serveErrs)
	return serveErrs
}

func (a *app) runRelay(start func(context.Context) error, serveErrs chan<- error) {
	a.log.Info("outbox relay starting", map[string]any{
		"poll_interval": a.cfg.OutboxPollInterval.String(),
		"batch_size":    a.cfg.OutboxBatchSize,
	})
	err := start(context.Background())
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	a.log.Error("outbox relay exited", map[string]any{"error": err.Error()})
	select {
	case serveErrs <- fmt.Errorf("outbox relay: %w", err):
	default:
	}
}

func (a *app) goLoop(ctx context.Context, run func(context.Context)) {
	a.loops.Add(1)
	go func() {
		defer a.loops.Done()
		run(ctx)
	}()
}

func (a *app) awaitLoops(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		a.loops.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		a.log.Warn("background loops still running at the shutdown deadline", map[string]any{"error": ctx.Err().Error()})
	}
}

func (a *app) stopServers(ctx context.Context) {
	a.cancelExporters()
	if err := a.httpServer.Shutdown(ctx); err != nil {
		a.log.Error("HTTP server shutdown error", map[string]any{"error": err.Error()})
	}
	if err := a.metricsServer.Shutdown(ctx); err != nil {
		a.log.Error("metrics server shutdown error", map[string]any{"error": err.Error()})
	}

	if err := a.outboxRelay.Stop(); err != nil {
		a.log.Error("outbox relay stop error", map[string]any{"error": err.Error()})
	}
	a.awaitLoops(ctx)

	if err := a.inboxPool.DrainAndClose(ctx); err != nil {
		a.log.Warn("inbox db pool drain timed out", map[string]any{"error": err.Error()})
	}
	if err := a.appPool.DrainAndClose(ctx); err != nil {
		a.log.Warn("app db pool drain timed out", map[string]any{"error": err.Error()})
	}
	if err := a.relayPool.DrainAndClose(ctx); err != nil {
		a.log.Warn("relay db pool drain timed out", map[string]any{"error": err.Error()})
	}
	if err := a.lockPool.DrainAndClose(ctx); err != nil {
		a.log.Warn("lock db pool drain timed out", map[string]any{"error": err.Error()})
	}
	if err := a.cache.Close(); err != nil {
		a.log.Error("valkey close error", map[string]any{"error": err.Error()})
	}
	a.sdkClient.Close()

	a.tracingShutdown()
}
