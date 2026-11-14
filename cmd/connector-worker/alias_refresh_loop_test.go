package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

const (
	cwcliFreshAliasBody  = `{"version":1,"restCall":[{"alias":"fresh","method":"GET","baseURL":"http://x","pathTemplate":"/y"}]}`
	cwcliAliasPath       = "/api/v1/internal/connector-aliases"
	cwcliRefreshFailMsg  = "connector alias refresh failed; keeping the previous registry"
	cwcliRefreshedMsg    = "connector alias registry refreshed"
	cwcliAliasAgeMetric  = "workflow_execution_service_connector_alias_registry_age_seconds"
	cwcliOldAliasPresent = "old"
)

func cwcliHeldAliases() aliasconfig.Config {
	return aliasconfig.Config{RestCall: []aliasconfig.Endpoint{
		{Alias: cwcliOldAliasPresent, Method: "GET", BaseURL: "http://x", PathTemplate: "/y"},
	}}
}

// cwcliHangingUpstream announces each request on arrived and holds it until the client gives up.
func cwcliHangingUpstream(t *testing.T) (*httptest.Server, chan cwcliRequest) {
	t.Helper()
	arrived := make(chan cwcliRequest, 16)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		arrived <- cwcliRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone()}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv, arrived
}

// cwcliAwaitRequest returns the next request the upstream saw, failing the test instead of hanging if none arrives.
func cwcliAwaitRequest(t *testing.T, arrived <-chan cwcliRequest) cwcliRequest {
	t.Helper()
	select {
	case got := <-arrived:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the upstream")
		return cwcliRequest{}
	}
}

func TestFetchAliases_ServerSlowerThanTheTimeout_FailsWithADeadlineErrorNamingTheURL(t *testing.T) {
	srv, arrived := cwcliHangingUpstream(t)

	_, err := fetchAliases(context.Background(), srv.URL+"/", 250*time.Millisecond, &cwcliLogger{})

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "fetch aliases from "+srv.URL+cwcliAliasPath)
	got := cwcliAwaitRequest(t, arrived)
	assert.Equal(t, http.MethodGet, got.method)
	assert.Equal(t, cwcliAliasPath, got.path)
	assert.Equal(t, "iam-system", got.header.Get("x-user-id"))
	assert.Equal(t, "iam-system", got.header.Get("x-tenant-roles"))
}

func TestFetchAliases_ConnectionRefused_NamesTheURLAndIsNotADeadline(t *testing.T) {
	_, err := fetchAliases(context.Background(), "http://127.0.0.1:1", 5*time.Second, &cwcliLogger{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch aliases from http://127.0.0.1:1"+cwcliAliasPath)
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
}

// Without a rest-call pool a fresh answer has nowhere to go: the held registry stays as it was, no refreshed line is
// logged, and the count of empty answers is back at zero.
func TestRefreshAliases_WithoutARestCallPool_AppliesNothingAndResetsTheEmptyCount(t *testing.T) {
	obstest.Init(t)
	upstream := newCwcliUpstream(t, http.StatusOK)
	upstream.setReply(cwcliFreshAliasBody)
	appliedAt := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	now := appliedAt.Add(90 * time.Second)
	d := &deps{
		pools:        map[string]*typePool{registry.TypeStorage: {}},
		aliases:      cwcliHeldAliases(),
		aliasesAt:    appliedAt,
		emptyAnswers: 2,
		now:          func() time.Time { return now },
	}
	cfg := &config.Config{DefinitionServiceURL: upstream.URL, ConnectorAliasFetchTimeout: time.Second}
	log := &cwcliLogger{}

	refreshAliases(context.Background(), d, cfg, log)

	reqs := upstream.requests()
	require.Len(t, reqs, 1, "the registry was fetched")
	assert.Equal(t, cwcliAliasPath, reqs[0].path)
	require.Len(t, d.currentAliases().RestCall, 1)
	assert.Equal(t, cwcliOldAliasPresent, d.currentAliases().RestCall[0].Alias, "the held registry is unchanged")
	assert.Zero(t, d.emptyAnswers)
	assert.Empty(t, log.withMessage("info", cwcliRefreshedMsg))
}

func TestRunAliasRefreshLoop_RefreshesOnEveryTickAndReturnsWhenItsContextIsCancelled(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == cwcliAliasPath && r.Method == http.MethodGet {
			hits.Add(1)
		}
		_, _ = w.Write([]byte(cwcliFreshAliasBody))
	}))
	t.Cleanup(srv.Close)
	pool := &typePool{}
	d := &deps{pools: map[string]*typePool{registry.TypeRestCall: pool}, aliases: cwcliHeldAliases()}
	cfg := &config.Config{DefinitionServiceURL: srv.URL, ConnectorAliasFetchTimeout: time.Second, ConnectorAliasRefreshInterval: time.Millisecond}
	log := &cwcliLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		runAliasRefreshLoop(ctx, d, cfg, log)
		close(done)
	}()

	require.Eventually(t, func() bool { return hits.Load() >= 3 }, 10*time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not return after its context was cancelled")
	}

	aliases := d.currentAliases()
	require.Len(t, aliases.RestCall, 1)
	assert.Equal(t, "fresh", aliases.RestCall[0].Alias, "a tick applies the fetched registry")
	assert.NotNil(t, pool.currentConnector(), "a tick rebuilds the rest-call connector")
	assert.NotEmpty(t, log.withMessage("info", cwcliRefreshedMsg))
}

// A refresh in flight when the context is cancelled fails on its cancelled request, is logged, and leaves the held
// registry in place.
func TestRunAliasRefreshLoop_RefreshInFlightAtCancel_IsLoggedAndKeepsTheRegistry(t *testing.T) {
	srv, arrived := cwcliHangingUpstream(t)
	pool := &typePool{}
	d := &deps{pools: map[string]*typePool{registry.TypeRestCall: pool}, aliases: cwcliHeldAliases()}
	cfg := &config.Config{DefinitionServiceURL: srv.URL, ConnectorAliasFetchTimeout: 30 * time.Second, ConnectorAliasRefreshInterval: time.Millisecond}
	log := &cwcliLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var finished sync.WaitGroup
	finished.Add(1)
	go func() {
		defer finished.Done()
		runAliasRefreshLoop(ctx, d, cfg, log)
	}()

	got := cwcliAwaitRequest(t, arrived)
	cancel()
	finished.Wait()

	assert.Equal(t, cwcliAliasPath, got.path)
	// select may still pick a ready tick after the cancel, so a second fetch can fail fast on the cancelled context.
	warns := log.withMessage("warn", cwcliRefreshFailMsg)
	require.NotEmpty(t, warns)
	for _, w := range warns {
		assert.Contains(t, w.fields["error"], "context canceled")
	}
	require.Len(t, d.currentAliases().RestCall, 1)
	assert.Equal(t, cwcliOldAliasPresent, d.currentAliases().RestCall[0].Alias)
	assert.Nil(t, pool.currentConnector(), "the connector is not rebuilt from a failed fetch")
}
