package service_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type fakeOutboxPruner struct {
	batches   []int64
	err       error
	calls     int
	olderThan time.Duration
	limit     int
}

func (f *fakeOutboxPruner) PrunePublished(_ context.Context, olderThan time.Duration, limit int) (int64, error) {
	f.olderThan, f.limit = olderThan, limit
	if f.err != nil {
		return 0, f.err
	}
	if f.calls >= len(f.batches) {
		return 0, nil
	}
	n := f.batches[f.calls]
	f.calls++
	return n, nil
}

type fakeInboxPruner struct {
	deleted   int64
	err       error
	calls     atomic.Int32
	olderThan time.Duration
}

func (f *fakeInboxPruner) PruneOlderThan(_ context.Context, olderThan time.Duration) (int64, error) {
	f.calls.Add(1)
	f.olderThan = olderThan
	return f.deleted, f.err
}

func newSweeper(outbox *fakeOutboxPruner, inbox *fakeInboxPruner, record func(table string, deleted int64)) *service.RetentionSweeper {
	return service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: outbox, ProcessedEvents: inbox,
		OutboxRetention: 8 * 24 * time.Hour, ProcessedEventRetention: 9 * 24 * time.Hour,
		Batch: 1000, Interval: time.Hour, Record: record, Logger: &fakeLogger{},
	})
}

func TestRetentionSweeper_PrunesOutboxInBatchesUntilAShortOne(t *testing.T) {
	outbox := &fakeOutboxPruner{batches: []int64{1000, 1000, 400}}
	inbox := &fakeInboxPruner{deleted: 7}
	deleted := map[string]int64{}
	sweeper := newSweeper(outbox, inbox, func(table string, n int64) { deleted[table] += n })

	require.NoError(t, sweeper.Tick(context.Background()))

	assert.Equal(t, 3, outbox.calls, "a full batch means more rows may wait, so the sweep goes on")
	assert.Equal(t, 8*24*time.Hour, outbox.olderThan)
	assert.Equal(t, 1000, outbox.limit)
	assert.Equal(t, 9*24*time.Hour, inbox.olderThan)
	assert.Equal(t, map[string]int64{"outbox_events": 2400, "processed_event": 7}, deleted)
}

func TestRetentionSweeper_OneTableFailingDoesNotStopTheOther(t *testing.T) {
	outbox := &fakeOutboxPruner{err: errors.New("relay pool down")}
	inbox := &fakeInboxPruner{deleted: 3}
	var inboxDeleted int64
	sweeper := newSweeper(outbox, inbox, func(table string, n int64) {
		if table == "processed_event" {
			inboxDeleted += n
		}
	})

	err := sweeper.Tick(context.Background())

	require.ErrorContains(t, err, "relay pool down")
	assert.EqualValues(t, 3, inboxDeleted)
}

func TestRetentionSweeper_ProcessedEventFailureIsReported(t *testing.T) {
	sweeper := newSweeper(&fakeOutboxPruner{}, &fakeInboxPruner{err: errors.New("db down")}, func(string, int64) {})

	require.ErrorContains(t, sweeper.Tick(context.Background()), "db down")
}

func TestRetentionSweeper_RunSweepsImmediatelyAndStopsOnCancel(t *testing.T) {
	inbox := &fakeInboxPruner{}
	sweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: &fakeOutboxPruner{}, ProcessedEvents: inbox,
		OutboxRetention: time.Hour, ProcessedEventRetention: time.Hour,
		Batch: 10, Interval: time.Millisecond, Record: func(string, int64) {}, Logger: &fakeLogger{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sweeper.Run(ctx); close(done) }()

	require.Eventually(t, func() bool { return inbox.calls.Load() >= 2 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after the context ended")
	}
}
