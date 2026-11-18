package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW16aPruneDown = errors.New("w16a: relay pool unavailable")

const w16aSweepBatch = 500

type w16aPruneCall struct {
	olderThan time.Duration
	limit     int
}

// w16aScriptedOutbox answers each call from results in order (a nil error and a count, or the failure),
// and records every call; calls past the script answer 0 and are recorded too.
type w16aScriptedOutbox struct {
	mu      sync.Mutex
	results []w16aPruneResult
	calls   []w16aPruneCall
}

type w16aPruneResult struct {
	deleted int64
	err     error
}

func (o *w16aScriptedOutbox) PrunePublished(_ context.Context, olderThan time.Duration, limit int) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	i := len(o.calls)
	o.calls = append(o.calls, w16aPruneCall{olderThan, limit})
	if i >= len(o.results) {
		return 0, nil
	}
	return o.results[i].deleted, o.results[i].err
}

func (o *w16aScriptedOutbox) recorded() []w16aPruneCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]w16aPruneCall(nil), o.calls...)
}

// w16aSignalInbox counts processed-event prunes and signals each one.
type w16aSignalInbox struct {
	mu     sync.Mutex
	calls  []w16aPruneCall
	pruned chan struct{}
}

func newW16aSignalInbox() *w16aSignalInbox { return &w16aSignalInbox{pruned: make(chan struct{}, 32)} }

func (i *w16aSignalInbox) PruneBatch(_ context.Context, olderThan time.Duration, batch int) (int64, error) {
	i.mu.Lock()
	i.calls = append(i.calls, w16aPruneCall{olderThan, batch})
	i.mu.Unlock()
	select {
	case i.pruned <- struct{}{}:
	default:
	}
	return 0, nil
}

func (i *w16aSignalInbox) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.calls)
}

func (i *w16aSignalInbox) recorded() []w16aPruneCall {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]w16aPruneCall(nil), i.calls...)
}

type w16aTableCount struct {
	table   string
	deleted int64
}

type w16aRecorder struct {
	mu      sync.Mutex
	entries []w16aTableCount
}

func (r *w16aRecorder) record(table string, deleted int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, w16aTableCount{table, deleted})
}

func (r *w16aRecorder) forTable(table string) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int64
	for _, e := range r.entries {
		if e.table == table {
			out = append(out, e.deleted)
		}
	}
	return out
}

func w16aNewSweeper(outbox *w16aScriptedOutbox, inbox *w16aSignalInbox, rec *w16aRecorder, interval time.Duration, log *w11aLogger) *service.RetentionSweeper {
	return service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: outbox, ProcessedEvents: inbox,
		OutboxRetention: 8 * 24 * time.Hour, ProcessedEventRetention: 9 * 24 * time.Hour,
		Batch: w16aSweepBatch, Interval: interval, Record: rec.record, Logger: log,
	})
}

func TestRetentionSweeper_OutboxPruneLoopsOnlyWhileACallDeletesAFullBatch(t *testing.T) {
	cases := []struct {
		name      string
		results   []w16aPruneResult
		wantCalls int
		wantCount []int64
	}{
		{"full, full, then short stops after three", []w16aPruneResult{{w16aSweepBatch, nil}, {w16aSweepBatch, nil}, {w16aSweepBatch - 1, nil}}, 3, []int64{w16aSweepBatch, w16aSweepBatch, w16aSweepBatch - 1}},
		{"a full batch then nothing stops after two", []w16aPruneResult{{w16aSweepBatch, nil}, {0, nil}}, 2, []int64{w16aSweepBatch, 0}},
		{"nothing to delete stops after one", []w16aPruneResult{{0, nil}}, 1, []int64{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &w16aScriptedOutbox{results: tc.results}
			inbox := newW16aSignalInbox()
			rec := &w16aRecorder{}

			require.NoError(t, w16aNewSweeper(outbox, inbox, rec, time.Hour, &w11aLogger{}).Tick(context.Background()))

			calls := outbox.recorded()
			require.Len(t, calls, tc.wantCalls)
			for _, c := range calls {
				assert.Equal(t, w16aPruneCall{8 * 24 * time.Hour, w16aSweepBatch}, c, "every call uses the outbox retention and the batch size")
			}
			assert.Equal(t, tc.wantCount, rec.forTable("outbox_events"), "each call's own count is recorded, not a running sum")
			assert.Equal(t, []w16aPruneCall{{9 * 24 * time.Hour, w16aSweepBatch}}, inbox.recorded(), "the processed events are pruned once, with their own retention and the batch")
			assert.Equal(t, []int64{0}, rec.forTable("processed_events"))
		})
	}
}

func TestRetentionSweeper_TickOutboxFailure_IsNamedKeepsItsCauseAndTheOtherTableStillRuns(t *testing.T) {
	cases := []struct {
		name      string
		results   []w16aPruneResult
		wantCalls int
		wantCount []int64
	}{
		{"the first call fails", []w16aPruneResult{{0, errW16aPruneDown}}, 1, nil},
		{"a call after a full batch fails", []w16aPruneResult{{w16aSweepBatch, nil}, {0, errW16aPruneDown}}, 2, []int64{w16aSweepBatch}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &w16aScriptedOutbox{results: tc.results}
			inbox := newW16aSignalInbox()
			rec := &w16aRecorder{}

			err := w16aNewSweeper(outbox, inbox, rec, time.Hour, &w11aLogger{}).Tick(context.Background())

			require.ErrorIs(t, err, errW16aPruneDown)
			assert.Equal(t, "prune published outbox events: "+errW16aPruneDown.Error(), err.Error(), "only the outbox failed, and the error names it")
			assert.Len(t, outbox.recorded(), tc.wantCalls, "the loop stops on the failure")
			assert.Equal(t, tc.wantCount, rec.forTable("outbox_events"), "a failed call records nothing for the outbox")
			assert.Equal(t, []w16aPruneCall{{9 * 24 * time.Hour, w16aSweepBatch}}, inbox.recorded(), "the processed-events prune still runs, with its own retention and the batch")
			for _, c := range outbox.recorded() {
				assert.Equal(t, w16aPruneCall{8 * 24 * time.Hour, w16aSweepBatch}, c, "the outbox calls use the outbox retention")
			}
			assert.Equal(t, []int64{0}, rec.forTable("processed_events"))
		})
	}
}

// w16aRunSweeper runs the sweeper until wantSweeps processed-event prunes have happened, then stops it.
func w16aRunSweeper(t *testing.T, sweeper *service.RetentionSweeper, inbox *w16aSignalInbox, wantSweeps int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sweeper.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	for i := 0; i < wantSweeps; i++ {
		select {
		case <-inbox.pruned:
		case <-time.After(w16aLoopWait):
			t.Fatalf("only %d sweeps ran, want %d", i, wantSweeps)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(w16aLoopWait):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestRetentionSweeper_Run_SweepsOnceAtStartBeforeTheFirstInterval(t *testing.T) {
	outbox := &w16aScriptedOutbox{}
	inbox := newW16aSignalInbox()
	rec := &w16aRecorder{}
	log := &w11aLogger{}

	w16aRunSweeper(t, w16aNewSweeper(outbox, inbox, rec, time.Hour, log), inbox, 1)

	assert.Len(t, outbox.recorded(), 1, "one outbox prune")
	assert.Equal(t, []w16aPruneCall{{9 * 24 * time.Hour, w16aSweepBatch}}, inbox.recorded(), "one processed-events prune with its own retention and the batch, and no second sweep inside the hour")
	assert.Equal(t, []w16aPruneCall{{8 * 24 * time.Hour, w16aSweepBatch}}, outbox.recorded(), "one outbox prune with the outbox retention")
	assert.Empty(t, log.at("error"))
}

func TestRetentionSweeper_Run_AFailingSweepIsLoggedAndTheNextSweepStillRuns(t *testing.T) {
	outbox := &w16aScriptedOutbox{results: []w16aPruneResult{{0, errW16aPruneDown}}}
	inbox := newW16aSignalInbox()
	rec := &w16aRecorder{}
	log := &w11aLogger{}

	w16aRunSweeper(t, w16aNewSweeper(outbox, inbox, rec, time.Millisecond, log), inbox, 2)

	calls := outbox.recorded()
	require.GreaterOrEqual(t, len(calls), 2, "the sweep after the failed one pruned the outbox again")
	for _, c := range calls {
		assert.Equal(t, w16aPruneCall{8 * 24 * time.Hour, w16aSweepBatch}, c)
	}
	require.GreaterOrEqual(t, inbox.count(), 2)
	for _, c := range inbox.recorded() {
		assert.Equal(t, w16aPruneCall{9 * 24 * time.Hour, w16aSweepBatch}, c)
	}
	errs := log.at("error")
	require.Len(t, errs, 1, "only the failing sweep is logged")
	assert.Equal(t, "retention sweep failed", errs[0].Msg)
	assert.Equal(t, map[string]any{"error": "prune published outbox events: " + errW16aPruneDown.Error()}, errs[0].Fields)
}
