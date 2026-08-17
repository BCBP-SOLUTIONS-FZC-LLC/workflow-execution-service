package main

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// newHealthServer builds cmd/connector-worker's plain net/http health/ready/
// metrics surface — mirrors cmd/worker's own newHealthServer shape. /readyz
// checks Valkey reachability only: this binary never dials Temporal at all
// (LLD workflow_connectors.md §6.1 Decision #2), so there is no SDK health
// check to run here the way cmd/server/cmd/worker both do.
func newHealthServer(addr string, d *deps) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", readyzHandler(d))
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{Addr: addr, Handler: mux}
}

func readyzHandler(d *deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := d.valkeyClient.Ping(ctx).Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
