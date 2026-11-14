package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type wkbootQueueRepo struct {
	port.ActiveTaskQueueRepository
	mu     sync.Mutex
	active []*domain.ActiveTaskQueue
	err    error
	calls  int
	onList func()
}

func (r *wkbootQueueRepo) ListActive(context.Context) ([]*domain.ActiveTaskQueue, error) {
	r.mu.Lock()
	r.calls++
	active, err, onList := r.active, r.err, r.onList
	r.mu.Unlock()
	if onList != nil {
		onList()
	}
	return active, err
}

type wkbootLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

type wkbootRecordingLog struct {
	mu      sync.Mutex
	entries []wkbootLogEntry
}

func (l *wkbootRecordingLog) record(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, wkbootLogEntry{level: level, msg: msg, fields: fields})
}

func (l *wkbootRecordingLog) Debug(msg string, f map[string]any) { l.record("debug", msg, f) }
func (l *wkbootRecordingLog) Info(msg string, f map[string]any)  { l.record("info", msg, f) }
func (l *wkbootRecordingLog) Warn(msg string, f map[string]any)  { l.record("warn", msg, f) }
func (l *wkbootRecordingLog) Error(msg string, f map[string]any) { l.record("error", msg, f) }

func (l *wkbootRecordingLog) all() []wkbootLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]wkbootLogEntry(nil), l.entries...)
}

func TestPollQueueTopology_PollsOnEveryTickUntilItsContextIsCancelledThenReturns(t *testing.T) {
	obstest.Init(t)
	called := make(chan struct{}, 64)
	repo := &wkbootQueueRepo{onList: func() {
		select {
		case called <- struct{}{}:
		default:
		}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollQueueTopology(ctx, nil, nil, repo, "wkboot-loop-default", time.Millisecond, newWorkerRegistry(), &wkbootRecordingLog{})
	}()

	for i := 1; i <= 2; i++ {
		select {
		case <-called:
		case <-time.After(30 * time.Second):
			t.Fatalf("ListActive was called %d times, want a poll on each tick", i-1)
		}
	}
	cancel()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("pollQueueTopology did not return after its context was cancelled")
	}
}

func TestPollQueueTopologyOnce_ListFailureWarnsAndChangesNothing(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "wkboot-listfail-default"
	registry := newWorkerRegistry()
	log := &wkbootRecordingLog{}
	repo := &wkbootQueueRepo{err: errors.New("queue table unreadable")}
	observability.WorkerActiveQueues.Set(7)
	defaultGauge := observability.WorkerQueueLastPollTimestamp.WithLabelValues(defaultQueue)
	defaultGauge.Set(1)

	pollQueueTopologyOnce(context.Background(), nil, nil, repo, defaultQueue, registry, log)

	entries := log.all()
	if len(entries) != 1 {
		t.Fatalf("log entries = %d (%v), want exactly one warning", len(entries), entries)
	}
	if e := entries[0]; e.level != "warn" || e.msg != "queue topology poll failed" || e.fields["error"] != "queue table unreadable" {
		t.Errorf("log entry = %+v, want a warn 'queue topology poll failed' carrying the repository error", e)
	}
	if got := len(registry.all()); got != 0 {
		t.Errorf("registry holds %d workers after a failed poll, want none", got)
	}
	if got := testutil.ToFloat64(observability.WorkerActiveQueues); got != 7 {
		t.Errorf("active queue gauge = %v after a failed poll, want it untouched at 7", got)
	}
	if got := testutil.ToFloat64(defaultGauge); got != 1 {
		t.Errorf("default queue poll timestamp = %v after a failed poll, want it untouched", got)
	}
}

func TestPollQueueTopologyOnce_SkipsTheDefaultQueueTheMainWorkerAlreadyServes(t *testing.T) {
	obstest.Init(t)
	const defaultQueue = "wkboot-skipdefault-default"
	registry := newWorkerRegistry()
	log := &wkbootRecordingLog{}
	repo := &wkbootQueueRepo{active: []*domain.ActiveTaskQueue{{TenantID: uuid.New(), QueueName: defaultQueue, Routable: true}}}
	observability.WorkerActiveQueues.Set(0)
	defaultGauge := observability.WorkerQueueLastPollTimestamp.WithLabelValues(defaultQueue)
	defaultGauge.Set(1)

	// A nil client makes any attempt to start a second worker panic.
	pollQueueTopologyOnce(context.Background(), nil, nil, repo, defaultQueue, registry, log)

	if registry.has(defaultQueue) {
		t.Error("the default queue was registered by the poll: it would get a second worker")
	}
	if got := len(registry.all()); got != 0 {
		t.Errorf("registry holds %d workers, want none", got)
	}
	if got := testutil.ToFloat64(observability.WorkerActiveQueues); got != 1 {
		t.Errorf("active queue gauge = %v, want 1 (the default queue counts as active)", got)
	}
	if got := testutil.ToFloat64(defaultGauge); got <= 1 {
		t.Errorf("default queue poll timestamp = %v, want it refreshed by the successful poll", got)
	}
	if entries := log.all(); len(entries) != 0 {
		t.Errorf("log entries = %v, want none for a list holding only the default queue", entries)
	}
}
