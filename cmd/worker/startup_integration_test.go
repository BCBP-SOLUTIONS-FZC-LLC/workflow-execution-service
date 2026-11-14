//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var (
	wkbootTemporalOnce   sync.Once
	wkbootTemporalClient client.Client
	wkbootTemporalErr    error
)

// wkbootSharedTemporal starts one Temporal dev server for the whole package
// run and returns a client on it. The container is not terminated here: the
// testcontainers reaper removes it when the test process exits. Tests that use
// it keep to their own queue and tenant ids.
func wkbootSharedTemporal(t *testing.T) client.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}
	wkbootTemporalOnce.Do(func() {
		ctx := context.Background()
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "temporalio/temporal:latest",
				Cmd:          []string{"server", "start-dev", "--ip", "0.0.0.0"},
				ExposedPorts: []string{"7233/tcp"},
				WaitingFor:   wait.ForListeningPort("7233/tcp").WithStartupTimeout(45 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			wkbootTemporalErr = fmt.Errorf("start temporal container: %w", err)
			return
		}
		host, err := container.Host(ctx)
		if err != nil {
			wkbootTemporalErr = fmt.Errorf("temporal container host: %w", err)
			return
		}
		mapped, err := container.MappedPort(ctx, "7233/tcp")
		if err != nil {
			wkbootTemporalErr = fmt.Errorf("temporal container port: %w", err)
			return
		}
		hostPort := host + ":" + mapped.Port()
		deadline := time.Now().Add(60 * time.Second)
		retry := time.NewTicker(time.Second)
		defer retry.Stop()
		for {
			sdk, err := client.Dial(client.Options{HostPort: hostPort})
			if err == nil {
				if _, healthErr := sdk.CheckHealth(ctx, &client.CheckHealthRequest{}); healthErr == nil {
					wkbootTemporalClient = sdk
					return
				}
				sdk.Close()
			}
			if time.Now().After(deadline) {
				wkbootTemporalErr = fmt.Errorf("temporal server never became healthy: %v", err)
				return
			}
			<-retry.C
		}
	})
	if wkbootTemporalErr != nil {
		t.Fatalf("shared temporal server: %v", wkbootTemporalErr)
	}
	return wkbootTemporalClient
}

const wkbootMissingNamespace = "wkboot-namespace-that-does-not-exist"

// wkbootClientWithoutNamespace shares the connection of a healthy server
// client but targets a namespace the server does not have, so a worker started
// on it fails.
func wkbootClientWithoutNamespace(t *testing.T, healthy client.Client) client.Client {
	t.Helper()
	c, err := client.NewClientFromExisting(healthy, client.Options{Namespace: wkbootMissingNamespace})
	if err != nil {
		t.Fatalf("client for a missing namespace: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func wkbootStopRegistered(t *testing.T, registry *workerRegistry) {
	t.Helper()
	t.Cleanup(func() {
		var wg sync.WaitGroup
		for _, w := range registry.all() {
			wg.Add(1)
			go func(w worker.Worker) {
				defer wg.Done()
				w.Stop()
			}(w)
		}
		wg.Wait()
	})
}

func TestStartWorkerForQueue_ReturnsNoWorkerAndNamesTheQueueWhenTheWorkerCannotStart(t *testing.T) {
	sdk := wkbootSharedTemporal(t)

	t.Run("a namespace the server lacks", func(t *testing.T) {
		queue := "wf-queue-" + uuid.NewString()
		bad := wkbootClientWithoutNamespace(t, sdk)

		w, err := startWorkerForQueue(bad, &outboundtemporal.Deps{}, queue, worker.Options{})

		if w != nil {
			t.Errorf("worker = %v, want nil when start fails", w)
		}
		if err == nil {
			t.Fatal("error = nil, want the start failure")
		}
		wantPrefix := `start worker for queue "` + queue + `": `
		if got := err.Error(); !strings.HasPrefix(got, wantPrefix) || got == wantPrefix {
			t.Errorf("error = %q, want it to start with %q followed by the cause", got, wantPrefix)
		}
		if errors.Unwrap(err) == nil {
			t.Error("error does not wrap the underlying start failure")
		}
	})

	t.Run("the same call against the healthy namespace starts a worker", func(t *testing.T) {
		queue := "wf-queue-" + uuid.NewString()

		w, err := startWorkerForQueue(sdk, &outboundtemporal.Deps{}, queue, worker.Options{})
		if err != nil {
			t.Fatalf("error = %v, want none", err)
		}
		t.Cleanup(w.Stop)
		if w == nil {
			t.Fatal("worker = nil, want a started worker")
		}
	})
}

func TestPollQueueTopologyOnce_StartsAndRegistersAWorkerForAQueueItDoesNotServeYet(t *testing.T) {
	obstest.Init(t)
	sdk := wkbootSharedTemporal(t)

	tenant := uuid.New()
	queue := domain.TenantTaskQueueName(tenant)
	registry := newWorkerRegistry()
	wkbootStopRegistered(t, registry)
	log := &wkbootRecordingLog{}
	repo := &wkbootQueueRepo{active: []*domain.ActiveTaskQueue{{TenantID: tenant, QueueName: queue, Routable: true}}}
	gauge := observability.WorkerQueueLastPollTimestamp.WithLabelValues(queue)
	gauge.Set(1)

	pollQueueTopologyOnce(context.Background(), sdk, &outboundtemporal.Deps{}, repo, "wkboot-start-default", registry, log)

	if !registry.has(queue) {
		t.Fatal("the new queue has no registered worker after the poll")
	}
	if got := len(registry.all()); got != 1 {
		t.Errorf("registry holds %d workers, want 1", got)
	}
	if got := testutil.ToFloat64(gauge); got <= 1 {
		t.Errorf("queue poll timestamp = %v, want it set when the worker started", got)
	}
	var started []wkbootLogEntry
	for _, e := range log.all() {
		if e.level == "info" && e.msg == "started worker for dynamic queue" {
			started = append(started, e)
		}
	}
	if len(started) != 1 {
		t.Fatalf("'started worker for dynamic queue' logs = %d, want 1 (all logs: %v)", len(started), log.all())
	}
	if started[0].fields["queue"] != queue || started[0].fields["tenant_id"] != tenant.String() {
		t.Errorf("log fields = %v, want queue %q and tenant_id %q", started[0].fields, queue, tenant)
	}
}

func TestPollQueueTopologyOnce_StartsAWorkerForAQueueStoppedFromRoutingSoItDrains(t *testing.T) {
	obstest.Init(t)
	sdk := wkbootSharedTemporal(t)

	tenant := uuid.New()
	queue := domain.TenantTaskQueueName(tenant)
	registry := newWorkerRegistry()
	wkbootStopRegistered(t, registry)
	log := &wkbootRecordingLog{}
	repo := &wkbootQueueRepo{active: []*domain.ActiveTaskQueue{{TenantID: tenant, QueueName: queue, Routable: false}}}

	pollQueueTopologyOnce(context.Background(), sdk, &outboundtemporal.Deps{}, repo, "wkboot-drain-default", registry, log)

	if !registry.has(queue) {
		t.Fatal("a queue with routable=false got no worker: its existing instances would stall")
	}
	for _, e := range log.all() {
		if e.level == "error" {
			t.Errorf("unexpected error log %q: %v", e.msg, e.fields)
		}
	}

	pollQueueTopologyOnce(context.Background(), sdk, &outboundtemporal.Deps{}, repo, "wkboot-drain-default", registry, log)

	if got := len(registry.all()); got != 1 {
		t.Errorf("registry holds %d workers after a second poll, want the one worker kept", got)
	}
}

func TestPollQueueTopologyOnce_QueueStartFailuresAreLoggedOneByOneLeftUnregisteredAndRetried(t *testing.T) {
	obstest.Init(t)
	sdk := wkbootSharedTemporal(t)
	bad := wkbootClientWithoutNamespace(t, sdk)

	first := domain.TenantTaskQueueName(uuid.New())
	second := domain.TenantTaskQueueName(uuid.New())
	registry := newWorkerRegistry()
	log := &wkbootRecordingLog{}
	repo := &wkbootQueueRepo{active: []*domain.ActiveTaskQueue{
		{TenantID: uuid.New(), QueueName: first, Routable: true},
		{TenantID: uuid.New(), QueueName: second, Routable: true},
	}}
	firstGauge := observability.WorkerQueueLastPollTimestamp.WithLabelValues(first)
	secondGauge := observability.WorkerQueueLastPollTimestamp.WithLabelValues(second)
	firstGauge.Set(1)
	secondGauge.Set(1)

	pollQueueTopologyOnce(context.Background(), bad, &outboundtemporal.Deps{}, repo, "wkboot-fail-default", registry, log)
	pollQueueTopologyOnce(context.Background(), bad, &outboundtemporal.Deps{}, repo, "wkboot-fail-default", registry, log)

	var failed []string
	for _, e := range log.all() {
		if e.level != "error" || e.msg != "failed to start worker for queue" {
			t.Errorf("unexpected log %s %q: %v", e.level, e.msg, e.fields)
			continue
		}
		if msg, _ := e.fields["error"].(string); msg == "" {
			t.Errorf("failure log for %v carries no error text", e.fields["queue"])
		}
		failed = append(failed, e.fields["queue"].(string))
	}
	want := []string{first, second, first, second}
	if len(failed) != len(want) {
		t.Fatalf("failure logs for queues %v, want %v: the second queue must be tried after the first failed, and both again on the next poll", failed, want)
	}
	for i := range want {
		if failed[i] != want[i] {
			t.Errorf("failure log %d is for %q, want %q", i, failed[i], want[i])
		}
	}
	if got := len(registry.all()); got != 0 {
		t.Errorf("registry holds %d workers, want none for queues that failed to start", got)
	}
	if testutil.ToFloat64(firstGauge) != 1 || testutil.ToFloat64(secondGauge) != 1 {
		t.Error("a poll timestamp was set for a queue whose worker failed to start")
	}
}

func TestBuildDeps_WiresTheRepositoriesTheQueueRepoAndTheDefinitionClientOnTheConfiguredDatabase(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)

	var mu sync.Mutex
	var gotPath, gotTenantQuery, gotTenantHeader string
	definition := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotPath, gotTenantQuery, gotTenantHeader = r.URL.Path, r.URL.Query().Get("tenant_id"), r.Header.Get("x-tenant-id")
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(definition.Close)

	cfg := &config.Config{
		DatabaseURL:             dsn,
		DefinitionServiceURL:    definition.URL,
		DefinitionClientTimeout: 5 * time.Second,
		PGMaxConns:              4,
		OTELServiceName:         "wkboot-worker",
	}
	log := &wkbootRecordingLog{}

	deps, queues, pool, cleanup, err := buildDeps(cfg, log)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(cleanup)

	if pool == nil || queues == nil {
		t.Fatalf("pool = %v, queue repo = %v, want both set", pool, queues)
	}
	if deps.Instances == nil || deps.Tasks == nil || deps.Assignments == nil || deps.Outbox == nil || deps.Transactor == nil || deps.Validator == nil || deps.Definitions == nil {
		t.Errorf("deps has an unset field: %+v", deps)
	}
	if deps.Log != port.Logger(log) {
		t.Error("deps.Log is not the logger passed to buildDeps")
	}

	tenant := uuid.New()
	queueName := domain.TenantTaskQueueName(tenant)
	registered, err := queues.Register(context.Background(), tenant, queueName)
	if err != nil {
		t.Fatalf("register a queue through the wired repo: %v", err)
	}
	active, err := queues.ListActive(context.Background())
	if err != nil {
		t.Fatalf("list active queues through the wired repo: %v", err)
	}
	var found bool
	for _, q := range active {
		if q.ID == registered.ID && q.TenantID == tenant && q.QueueName == queueName {
			found = true
		}
	}
	if !found {
		t.Errorf("queue %q registered through the wired repo is not listed as active: the repo is not on the configured database", queueName)
	}

	version := uuid.New()
	_, err = deps.Definitions.GetCompiledWorkflow(context.Background(), tenant, version)
	if !errors.Is(err, port.ErrDefinitionRejected) {
		t.Fatalf("definition call error = %v, want the 404 mapped to ErrDefinitionRejected", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := "/api/v1/internal/workflow-versions/" + version.String() + "/compiled-plan"; gotPath != want {
		t.Errorf("definition request path = %q, want %q under the configured DEFINITION_SERVICE_URL", gotPath, want)
	}
	if gotTenantQuery != tenant.String() || gotTenantHeader != tenant.String() {
		t.Errorf("definition request tenant query %q, header %q, want both %q", gotTenantQuery, gotTenantHeader, tenant)
	}
}
