// Command tokenservice is a local stand-in for the Token Service that answers every tenant's
// platform-automation service account as active, for the black-box checks in test/hurl.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	defaultAddr   = ":8085"
	principalID   = "018f2d3c-0000-7000-8000-0000000000aa"
	principalSub  = "service-account:platform-automation"
	principalPath = "GET /api/v1/internal/tenants/{tenant}/service-accounts/platform-automation"
)

type principalResponse struct {
	Status       string `json:"status"`
	PrincipalID  string `json:"principal_id"`
	PrincipalSub string `json:"principal_sub"`
}

func main() {
	addr := os.Getenv("STUB_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	mux := http.NewServeMux()
	mux.HandleFunc(principalPath, handlePrincipal)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	fmt.Fprintf(os.Stderr, "token service stub listening on %s\n", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "token service stub: %v\n", err)
		os.Exit(1)
	}
}

func handlePrincipal(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(os.Stderr, "request %s %s\n", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	body := principalResponse{Status: "active", PrincipalID: principalID, PrincipalSub: principalSub}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		fmt.Fprintf(os.Stderr, "write principal: %v\n", err)
	}
}
