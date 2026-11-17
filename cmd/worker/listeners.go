package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

type listeners struct {
	health  net.Listener
	metrics net.Listener
}

func bindListeners(ctx context.Context, healthAddr, metricsAddr string) (*listeners, error) {
	var lc net.ListenConfig
	health, err := lc.Listen(ctx, "tcp", healthAddr)
	if err != nil {
		return nil, fmt.Errorf("bind WORKER_HEALTH_PORT %s: %w", healthAddr, err)
	}
	metrics, err := lc.Listen(ctx, "tcp", metricsAddr)
	if err != nil {
		_ = health.Close()
		return nil, fmt.Errorf("bind WORKER_METRICS_PORT %s: %w", metricsAddr, err)
	}
	return &listeners{health: health, metrics: metrics}, nil
}

func (l *listeners) close() {
	_ = l.health.Close()
	_ = l.metrics.Close()
}

func serveOn(name string, srv *http.Server, ln net.Listener, log port.Logger) {
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(name+" server exited", map[string]any{"error": err.Error()})
		}
	}()
}

func temporalDialOptions(cfg *config.Config) (client.Options, error) {
	interceptors, err := observability.TracingClientInterceptors(cfg.TraceExportEnabled())
	if err != nil {
		return client.Options{}, err
	}
	return temporalclient.DialOptions(cfg.TemporalHostPort, cfg.TemporalNamespace, cfg.TemporalTLSCertPath, cfg.TemporalTLSKeyPath, interceptors)
}
