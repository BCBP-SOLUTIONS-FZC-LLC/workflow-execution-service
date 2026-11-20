package main

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type topoFakeWorker struct {
	worker.Worker
	mu      sync.Mutex
	stopped int
}

func (w *topoFakeWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped++
}

func (w *topoFakeWorker) stopCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

type topoStarter struct {
	mu      sync.Mutex
	started map[string]*topoFakeWorker
	failing map[string]bool
	onStart func(queue string)
}

func useTopoStarter(t *testing.T, failing ...string) *topoStarter {
	t.Helper()
	s := &topoStarter{started: map[string]*topoFakeWorker{}, failing: map[string]bool{}}
	for _, q := range failing {
		s.failing[q] = true
	}
	previous := startQueueWorker
	startQueueWorker = func(_ client.Client, _ *outboundtemporal.Deps, queue string, _ worker.Options) (worker.Worker, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failing[queue] {
			return nil, errors.New("namespace not found")
		}
		w := &topoFakeWorker{}
		s.started[queue] = w
		if s.onStart != nil {
			s.onStart(queue)
		}
		return w, nil
	}
	t.Cleanup(func() { startQueueWorker = previous })
	return s
}

func topoActive(tenants ...uuid.UUID) []*domain.ActiveTaskQueue {
	var out []*domain.ActiveTaskQueue
	for _, tenant := range tenants {
		out = append(out, &domain.ActiveTaskQueue{TenantID: tenant, QueueName: domain.TenantTaskQueueName(tenant), Routable: true})
	}
	return out
}

func topoQueueSeriesExists(t *testing.T, queue string) bool {
	t.Helper()
	families, err := gincommon.MetricsGatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "workflow_execution_service_worker_queue_last_poll_timestamp" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "queue" && lp.GetValue() == queue {
					return true
				}
			}
		}
	}
	return false
}

func TestPollQueueTopology_ReadsTheRegistryAtStartBeforeTheFirstInterval(t *testing.T) {
	obstest.Init(t)
	tenant := uuid.New()
	queue := domain.TenantTaskQueueName(tenant)
	started := make(chan struct{})
	starter := useTopoStarter(t)
	starter.onStart = func(string) { close(started) }
	repo := &wkbootQueueRepo{active: topoActive(tenant)}
	registry := newWorkerRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollQueueTopology(ctx, nil, nil, repo, "topo-initial-default", time.Hour, registry, quietLogger{})
	}()

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("no worker started for the tenant queue: the first registry read waited for the interval")
	}
	cancel()
	<-done

	if !registry.has(queue) {
		t.Errorf("queue %q has no registered worker after the initial read", queue)
	}
	if repo.calls != 1 {
		t.Errorf("registry reads = %d, want exactly the initial one: the hour-long interval never elapsed", repo.calls)
	}
}

func TestPollQueueTopologyOnce_AQueueGoneFromTheRegistryHasItsWorkerStoppedAndItsSeriesRemoved(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "topo-gone-default"
	kept, gone := uuid.New(), uuid.New()
	keptQueue, goneQueue := domain.TenantTaskQueueName(kept), domain.TenantTaskQueueName(gone)
	keptWorker, goneWorker, defaultWorker := &topoFakeWorker{}, &topoFakeWorker{}, &topoFakeWorker{}
	registry := newWorkerRegistry()
	registry.add(defaultQueue, defaultWorker)
	registry.add(keptQueue, keptWorker)
	registry.add(goneQueue, goneWorker)
	repo := &wkbootQueueRepo{active: topoActive(kept)}

	pollQueueTopologyOnce(context.Background(), nil, nil, repo, defaultQueue, registry, quietLogger{})

	if goneWorker.stopCount() != 1 {
		t.Errorf("worker of the purged queue stopped %d times, want once", goneWorker.stopCount())
	}
	if registry.has(goneQueue) {
		t.Error("the purged queue is still registered, so its worker is stopped again on every poll")
	}
	if topoQueueSeriesExists(t, goneQueue) || topoQueueSeriesExists(t, keptQueue) {
		t.Error("a queue name is exported as a label: tenant ids must stay out of metric labels")
	}
	if !topoQueueSeriesExists(t, observability.QueueClassDedicated) {
		t.Error("the dedicated series is gone while a dedicated queue is still served")
	}
	if keptWorker.stopCount() != 0 || defaultWorker.stopCount() != 0 {
		t.Errorf("stops of the listed queue's worker = %d and of the default worker = %d, want none", keptWorker.stopCount(), defaultWorker.stopCount())
	}
	if !registry.has(keptQueue) || !registry.has(defaultQueue) {
		t.Error("a worker for a still-listed queue or the default queue was dropped")
	}
}

func TestPollQueueTopologyOnce_TheDedicatedSeriesGoesWithTheLastDedicatedQueue(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "topo-last-default"
	gone := domain.TenantTaskQueueName(uuid.New())
	registry := newWorkerRegistry()
	registry.add(defaultQueue, &topoFakeWorker{})
	registry.add(gone, &topoFakeWorker{})
	observability.WorkerQueueLastPollTimestamp.WithLabelValues(observability.QueueClassDedicated).Set(1)
	repo := &wkbootQueueRepo{}

	pollQueueTopologyOnce(context.Background(), nil, nil, repo, defaultQueue, registry, quietLogger{})

	if topoQueueSeriesExists(t, observability.QueueClassDedicated) {
		t.Error("the dedicated series is still exported with no dedicated queue served: the stalled-queue alert would page for it")
	}
	if !topoQueueSeriesExists(t, observability.QueueClassDefault) {
		t.Error("the default series is missing after a successful poll")
	}
}

func TestPollQueueTopologyOnce_AFailedRegistryReadStopsNoWorker(t *testing.T) {
	obstest.Init(t)
	queue := domain.TenantTaskQueueName(uuid.New())
	w := &topoFakeWorker{}
	registry := newWorkerRegistry()
	registry.add(queue, w)
	repo := &wkbootQueueRepo{err: errors.New("queue table unreadable")}

	pollQueueTopologyOnce(context.Background(), nil, nil, repo, "topo-readfail-default", registry, quietLogger{})

	if w.stopCount() != 0 || !registry.has(queue) {
		t.Errorf("worker stopped %d times, registered %v: an unreadable registry must not retire queues", w.stopCount(), registry.has(queue))
	}
}

func TestPollQueueTopologyOnce_TheActiveQueuesGaugeCountsRunningWorkersNotRowsRead(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "topo-gauge-default"
	ok, failing, alsoFailing := uuid.New(), uuid.New(), uuid.New()
	useTopoStarter(t, domain.TenantTaskQueueName(failing), domain.TenantTaskQueueName(alsoFailing))
	registry := newWorkerRegistry()
	registry.add(defaultQueue, &topoFakeWorker{})
	repo := &wkbootQueueRepo{active: topoActive(ok, failing, alsoFailing)}
	observability.WorkerActiveQueues.Set(99)

	pollQueueTopologyOnce(context.Background(), nil, nil, repo, defaultQueue, registry, quietLogger{})

	if got := testutil.ToFloat64(observability.WorkerActiveQueues); got != 2 {
		t.Errorf("active queues gauge = %v, want 2: the default worker and the one that started, not the two that failed", got)
	}
}

func TestPollQueueTopologyOnce_TheActiveQueuesGaugeDropsWhenAQueueIsRetired(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "topo-gaugedrop-default"
	gone := uuid.New()
	registry := newWorkerRegistry()
	registry.add(defaultQueue, &topoFakeWorker{})
	registry.add(domain.TenantTaskQueueName(gone), &topoFakeWorker{})
	observability.WorkerActiveQueues.Set(2)

	pollQueueTopologyOnce(context.Background(), nil, nil, &wkbootQueueRepo{}, defaultQueue, registry, quietLogger{})

	if got := testutil.ToFloat64(observability.WorkerActiveQueues); got != 1 {
		t.Errorf("active queues gauge = %v, want 1 (only the default worker runs)", got)
	}
}

func TestStopTopology_AWorkerStartInFlightAtShutdownIsStoppedBeforeShutdownReturns(t *testing.T) {
	obstest.Init(t)
	tenant := uuid.New()
	queue := domain.TenantTaskQueueName(tenant)
	release := make(chan struct{})
	inFlight := make(chan struct{})
	created := make(chan *topoFakeWorker, 1)
	previous := startQueueWorker
	startQueueWorker = func(client.Client, *outboundtemporal.Deps, string, worker.Options) (worker.Worker, error) {
		close(inFlight)
		<-release
		w := &topoFakeWorker{}
		created <- w
		return w, nil
	}
	t.Cleanup(func() { startQueueWorker = previous })
	registry := newWorkerRegistry()
	repo := &wkbootQueueRepo{active: topoActive(tenant)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		pollQueueTopology(ctx, nil, nil, repo, "topo-shutdown-default", time.Hour, registry, quietLogger{})
	}()
	<-inFlight

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stopTopology(cancel, pollDone, registry)
	}()
	<-ctx.Done()
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}
	select {
	case <-stopped:
		t.Fatal("shutdown returned while a worker start was still in flight")
	default:
	}
	close(release)
	<-stopped

	w := <-created
	if w.stopCount() != 1 {
		t.Errorf("worker started during shutdown was stopped %d times before shutdown returned, want once", w.stopCount())
	}
	if !registry.has(queue) && w.stopCount() == 0 {
		t.Error("the worker is neither registered nor stopped: it leaked")
	}
}

type topoSlowStopWorker struct {
	worker.Worker
	stopping chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (w *topoSlowStopWorker) Stop() {
	close(w.stopping)
	<-w.release
	close(w.finished)
}

func TestStopTopology_AWorkerBeingRetiredIsWaitedForBeforeShutdownReturns(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "topo-retire-default"
	gone := domain.TenantTaskQueueName(uuid.New())
	slow := &topoSlowStopWorker{stopping: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	registry := newWorkerRegistry()
	registry.add(defaultQueue, &topoFakeWorker{})
	registry.add(gone, slow)
	repo := &wkbootQueueRepo{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		pollQueueTopology(ctx, nil, nil, repo, defaultQueue, time.Hour, registry, quietLogger{})
	}()
	<-slow.stopping

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stopTopology(cancel, pollDone, registry)
	}()
	<-ctx.Done()
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}
	select {
	case <-stopped:
		t.Fatal("shutdown returned while a retiring worker was still stopping")
	default:
	}
	close(slow.release)
	<-stopped

	select {
	case <-slow.finished:
	default:
		t.Error("shutdown returned while a retiring worker was still stopping")
	}
}
