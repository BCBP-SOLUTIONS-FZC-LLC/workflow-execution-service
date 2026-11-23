package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

const w19aWaitLimit = 30 * time.Second

// w19aCleanEnv gives the test a working directory without a .env file and removes every inherited variable
// (not merely empties it), so the machine's own settings cannot change what Load returns. Not for parallel tests.
func w19aCleanEnv(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if !found || key == "" {
			continue
		}
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
}

// w19aRunGuarded runs loop in a goroutine. A panic inside it is caught and handed back with the done channel, so
// a missing guard fails one test instead of ending the test binary. panicked is safe to read once done is closed.
func w19aRunGuarded(loop func()) (done <-chan struct{}, panicked *any) {
	finished := make(chan struct{})
	var recovered any
	go func() {
		defer close(finished)
		defer func() { recovered = recover() }()
		loop()
	}()
	return finished, &recovered
}

func TestRunAliasRefreshLoop_NonPositiveInterval_ReturnsAtOnceWithoutFetching(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
	}{
		{"zero", 0},
		{"negative", -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fetches atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fetches.Add(1)
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)
			d := &deps{pools: map[string]*typePool{}}
			cfg := &config.Config{DefinitionServiceURL: srv.URL, ConnectorAliasFetchTimeout: time.Second, ConnectorAliasRefreshInterval: tc.interval}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			done, panicked := w19aRunGuarded(func() { runAliasRefreshLoop(ctx, d, cfg, noopLogger{}) })

			select {
			case <-done:
			case <-time.After(w19aWaitLimit):
				t.Fatal("the loop kept running with a non-positive interval and a live context")
			}

			require.Nil(t, *panicked, "the loop must not build a ticker from a non-positive interval")
			assert.Zero(t, fetches.Load(), "a loop that never started fetched nothing")
		})
	}
}

func TestRunMaintenanceLoop_NonPositiveAliasInterval_RunsOnWithoutResettingClients(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
	}{
		{"zero", 0},
		{"negative", -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			caching := &resettableConnector{countingConnector: countingConnector{typ: "caching"}}
			pruner := &fakePruner{batches: []int64{0}}
			d := &deps{pruner: pruner, pools: map[string]*typePool{"caching": {connectorType: "caching", connector: caching}}}
			cfg := &config.Config{ConnectorAliasRefreshInterval: tc.interval, ConnectorAuditRetention: time.Hour}
			ctx, cancel := context.WithCancel(context.Background())
			done, panicked := w19aRunGuarded(func() { runMaintenanceLoop(ctx, d, cfg, noopLogger{}) })
			t.Cleanup(func() {
				cancel()
				<-done
			})
			ended := func() bool {
				select {
				case <-done:
					return true
				default:
					return false
				}
			}

			require.Eventually(t, func() bool { return pruner.pruneCount() == 1 || ended() }, w19aWaitLimit, 5*time.Millisecond, "the loop reached its first prune")

			if ended() {
				require.Nil(t, *panicked, "the loop must not build a ticker from a non-positive interval")
				t.Fatal("the loop returned on its own")
			}
			assert.Zero(t, caching.resetCount(), "no reset at start-up and no panic")
		})
	}
}

// With no OPENBAO_TOKEN the connector-worker passes its start-up checks in dev and local, and every secret read
// then goes out with an empty token and is refused.
func TestConnectorWorker_EmptyOpenBaoTokenInDevAndLocal_StartsAndEveryReadIsUnauthorized(t *testing.T) {
	type w19aRequest struct {
		method string
		path   string
		tokens []string
	}
	for _, appEnv := range []string{"dev", "local"} {
		t.Run(appEnv, func(t *testing.T) {
			var mu sync.Mutex
			var requests []w19aRequest
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, w19aRequest{method: r.Method, path: r.URL.Path, tokens: r.Header.Values("X-Vault-Token")})
				mu.Unlock()
				w.WriteHeader(http.StatusForbidden)
			}))
			t.Cleanup(srv.Close)
			w19aCleanEnv(t)
			t.Setenv("APP_ENV", appEnv)
			t.Setenv("DEFINITION_SERVICE_URL", "http://definition:8080")
			t.Setenv("DATABASE_URL", "postgres://x")
			t.Setenv("OPENBAO_ADDR", srv.URL)
			t.Setenv("OPENBAO_TOKEN", "")
			t.Setenv("EXECUTION_SERVICE_INTERNAL_ADDR", "http://execution:8080")

			cfg, err := config.Load()
			require.NoError(t, err)
			require.NoError(t, cfg.RequireConnectorWorker(), "the worker passes its config check without a token in %s", appEnv)
			require.NoError(t, checkRequiredConfig(cfg), "the worker passes its start-up check without a token in %s", appEnv)
			reader := openbao.NewReader(cfg.OpenBaoAddr, cfg.OpenBaoToken, cfg.OpenBaoMount, cfg.OpenBaoTimeout)

			for _, path := range []string{"connectors/acme/rest/first", "connectors/acme/rest/second"} {
				_, err = reader.Read(context.Background(), path, "value")
				require.ErrorIs(t, err, openbao.ErrUnauthorized, "read of %s", path)
			}

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, []w19aRequest{
				{method: http.MethodGet, path: "/v1/secret/data/connectors/acme/rest/first", tokens: []string{""}},
				{method: http.MethodGet, path: "/v1/secret/data/connectors/acme/rest/second", tokens: []string{""}},
			}, requests, "each read was a GET of its secret that carried the X-Vault-Token header, empty")
		})
	}
}
