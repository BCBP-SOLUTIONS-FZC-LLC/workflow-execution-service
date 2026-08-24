package main

import (
	"context"
	"net/http"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
)

// newHealthServer builds cmd/worker's plain net/http health/ready surface
// (LLD-mandated for both binaries). No gin dependency exists in this binary
// today and two static-shaped endpoints don't justify adding one. /metrics
// lives on its own listener (newMetricsServer) so a NetworkPolicy port rule
// can isolate Prometheus scraping from liveness/readiness probes.
func newHealthServer(addr string, pool *pgcommon.Pool, sdk client.Client) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", readyzHandler(pool, sdk))
	return &http.Server{Addr: addr, Handler: mux}
}

// newMetricsServer builds cmd/worker's dedicated /metrics listener, split
// from newHealthServer's health/ready surface — mirrors cmd/server's own
// httpServer/metricsServer split (cmd/server/wire.go).
func newMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{Addr: addr, Handler: mux}
}

func readyzHandler(pool *pgcommon.Pool, sdk client.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if hs := pool.Health(ctx); !hs.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		healthCtx, healthCancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer healthCancel()
		if _, err := sdk.CheckHealth(healthCtx, &client.CheckHealthRequest{}); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}
