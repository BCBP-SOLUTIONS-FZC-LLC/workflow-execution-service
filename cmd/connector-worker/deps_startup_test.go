package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

// cwdepsConnector answers every call with a fixed output.
type cwdepsConnector struct {
	typ   string
	out   map[string]any
	calls atomic.Int32
}

func (c *cwdepsConnector) Type() string { return c.typ }
func (c *cwdepsConnector) Execute(context.Context, map[string]any) (map[string]any, error) {
	c.calls.Add(1)
	return c.out, nil
}

func cwdepsAliasServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// cwdepsConnectionCounter listens on a local port and counts every connection
// made to it, standing in for a Valkey the worker must not have dialled.
func cwdepsConnectionCounter(t *testing.T) (addr string, stop func() int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var count atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			count.Add(1)
			_ = conn.Close()
		}
	}()
	var once sync.Once
	stop = func() int32 {
		once.Do(func() {
			_ = ln.Close()
			<-done
		})
		return count.Load()
	}
	t.Cleanup(func() { stop() })
	return ln.Addr().String(), stop
}

func TestBuildDeps_FirstAliasFetchFails_ReturnsWrappedErrorBeforeOpeningValkey(t *testing.T) {
	cases := []struct {
		name       string
		serverURL  func(t *testing.T) string
		wantDetail string
	}{
		{
			name:       "definition service answers 500",
			serverURL:  func(t *testing.T) string { return cwdepsAliasServer(t, http.StatusInternalServerError, `{}`).URL },
			wantDetail: "status 500",
		},
		{
			name:       "definition service unreachable",
			serverURL:  func(*testing.T) string { return "http://127.0.0.1:1" },
			wantDetail: "fetch aliases from http://127.0.0.1:1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valkeyAddr, connections := cwdepsConnectionCounter(t)
			cfg := &config.Config{
				DefinitionServiceURL:       tc.serverURL(t),
				ConnectorAliasFetchTimeout: 2 * time.Second,
				ValkeyAddr:                 valkeyAddr,
				ValkeyDialTimeout:          time.Second,
			}

			d, cleanup, err := buildDeps(cfg, noopLogger{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "fetch alias registry: ")
			assert.Contains(t, err.Error(), tc.wantDetail)
			assert.Nil(t, d)
			assert.Nil(t, cleanup)
			assert.Zero(t, connections(), "Valkey was dialled although the alias fetch failed")
		})
	}
}

func TestBuildDeps_EmptyButValidRegistry_GoesOnToOpenValkey(t *testing.T) {
	srv := cwdepsAliasServer(t, http.StatusOK, `{"version":1,"restCall":[],"sqlQuery":[]}`)
	cfg := &config.Config{
		DefinitionServiceURL:       srv.URL,
		ConnectorAliasFetchTimeout: 2 * time.Second,
		ValkeyAddr:                 "127.0.0.1:1",
		ValkeyDialTimeout:          time.Second,
	}

	_, _, err := buildDeps(cfg, noopLogger{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey ping", "an empty registry must not stop startup at the alias fetch")
	assert.NotContains(t, err.Error(), "fetch alias registry")
}

func TestBuildPools_NonPositiveSize_GivesThatTypeAPoolOfOne(t *testing.T) {
	cfg := &config.Config{
		ConnectorPoolSizeStorage:  3,
		ConnectorPoolSizeRestCall: 0,
		ConnectorPoolSizeSQLQuery: -4,
		ConnectorTimeoutRestCall:  7 * time.Second,
	}
	set := map[string]connectors.Connector{
		registry.TypeStorage:  &cwdepsConnector{typ: registry.TypeStorage},
		registry.TypeRestCall: &cwdepsConnector{typ: registry.TypeRestCall},
		registry.TypeSQLQuery: &cwdepsConnector{typ: registry.TypeSQLQuery},
	}

	pools := buildPools(cfg, set)

	require.Len(t, pools, 3)
	assert.Equal(t, 3, cap(pools[registry.TypeStorage].sem), "a positive size is kept")
	assert.Equal(t, 1, cap(pools[registry.TypeRestCall].sem), "size 0")
	assert.Equal(t, 1, cap(pools[registry.TypeSQLQuery].sem), "negative size")
	assert.Equal(t, 7*time.Second, pools[registry.TypeRestCall].timeout)
	assert.Same(t, set[registry.TypeRestCall], pools[registry.TypeRestCall].currentConnector())
}

func TestWithJitter_AddsBetweenNoneAndHalfOfTheWait(t *testing.T) {
	const samples = 2000
	for _, base := range []time.Duration{100 * time.Millisecond, 300 * time.Millisecond} {
		var above int
		for range samples {
			got := withJitter(base)
			require.GreaterOrEqual(t, got, base)
			require.LessOrEqual(t, got, base+base/2)
			if got > base {
				above++
			}
		}
		assert.Greater(t, above, samples/2, "jitter is added to most waits (base %s)", base)
	}
	assert.Equal(t, time.Duration(0), withJitter(0))
}

// Every goroutine starts at the same moment, so claim, setDeadline, live and
// remove really overlap; run with -race.
func TestInflightSet_ConcurrentUse_KeepsEveryEntryConsistent(t *testing.T) {
	const workers = 32
	var set inflightSet
	start := make(chan struct{})
	var wg sync.WaitGroup
	var winners atomic.Int32
	deadline := time.Unix(1_000, 0)

	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			own := uuid.NewString()
			assert.True(t, set.claim(own))
			set.setDeadline(own, deadline)
			_ = set.live(deadline.Add(-time.Second))
			if set.claim("shared") {
				winners.Add(1)
			}
			if i%2 == 0 {
				set.remove(own)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), winners.Load(), "exactly one goroutine may claim the shared id")
	live := set.live(deadline.Add(-time.Second))
	assert.Len(t, live, workers/2+1, "the odd workers' ids and the shared id remain")
	assert.Equal(t, []string{"shared"}, set.live(deadline.Add(time.Second)), "only the entry with no deadline outlives the others' deadlines")
	set.remove("shared")
	assert.True(t, set.claim("shared"), "a removed id can be claimed again")
}

// Readers use the registry and connector while the refresh replaces them.
func TestRefreshAliases_ConcurrentWithReaders_AppliesTheLatestRegistry(t *testing.T) {
	srv := cwdepsAliasServer(t, http.StatusOK, `{"version":1,"restCall":[{"alias":"a","method":"GET","baseURL":"http://x","pathTemplate":"/y","timeout":5000000000}],"sqlQuery":[]}`)
	pool := &typePool{connectorType: registry.TypeRestCall, sem: make(chan struct{}, 1), timeout: time.Second, connector: &cwdepsConnector{typ: registry.TypeRestCall}}
	d := &deps{pools: map[string]*typePool{registry.TypeRestCall: pool}}
	cfg := &config.Config{DefinitionServiceURL: srv.URL, ConnectorAliasFetchTimeout: 2 * time.Second}

	const refreshes, readers = 20, 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range refreshes {
				_ = d.currentAliases()
				_ = d.aliasAge()
				assert.NotNil(t, pool.currentConnector())
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range refreshes {
			refreshAliases(context.Background(), d, cfg, noopLogger{})
		}
	}()
	close(start)
	wg.Wait()

	require.Len(t, d.currentAliases().RestCall, 1)
	assert.Equal(t, "a", d.currentAliases().RestCall[0].Alias)
}

// cwdepsHoldingConnector blocks inside Execute until released.
type cwdepsHoldingConnector struct {
	cwdepsConnector
	started, release chan struct{}
}

func (c *cwdepsHoldingConnector) Execute(ctx context.Context, in map[string]any) (map[string]any, error) {
	close(c.started)
	<-c.release
	return c.cwdepsConnector.Execute(ctx, in)
}

// A job holds the connector it fetched, so a refresh that swaps the pool's
// connector while it runs changes neither its call nor its outcome.
func TestExecute_RefreshDuringARun_DoesNotAffectTheRunningJob(t *testing.T) {
	srv := cwdepsAliasServer(t, http.StatusOK, `{"version":1,"restCall":[{"alias":"a","method":"GET","baseURL":"http://x","pathTemplate":"/y","timeout":5000000000}],"sqlQuery":[]}`)
	held := &cwdepsHoldingConnector{
		cwdepsConnector: cwdepsConnector{typ: registry.TypeRestCall, out: map[string]any{"from": "held"}},
		started:         make(chan struct{}), release: make(chan struct{}),
	}
	pool := &typePool{connectorType: registry.TypeRestCall, sem: make(chan struct{}, 1), timeout: 5 * time.Second, connector: held}
	d := &deps{pools: map[string]*typePool{registry.TypeRestCall: pool}, retry: retryBounds{MaxAttempts: 1}}
	cfg := &config.Config{DefinitionServiceURL: srv.URL, ConnectorAliasFetchTimeout: 2 * time.Second}
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), connectorType: registry.TypeRestCall}

	result := make(chan dispatchOutcome, 1)
	go func() { result <- execute(context.Background(), d, pool, job, noopLogger{}) }()
	<-held.started
	refreshAliases(context.Background(), d, cfg, noopLogger{})
	require.True(t, pool.currentConnector() != connectors.Connector(held), "the refresh replaced the pool's connector")
	require.Len(t, d.currentAliases().RestCall, 1)
	close(held.release)

	outcome := <-result
	assert.Empty(t, outcome.ErrorClass)
	assert.Equal(t, map[string]any{"from": "held"}, outcome.Output)
	assert.Equal(t, int32(1), held.calls.Load())
}
