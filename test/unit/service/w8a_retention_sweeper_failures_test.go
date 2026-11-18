package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW8aOutboxPrune = errors.New("w8a: outbox prune failed")
	errW8aInboxPrune  = errors.New("w8a: processed-events prune failed")
)

const (
	w8aSweepBatch           = 2
	w8aSweepOutboxRetention = 8 * 24 * time.Hour
	w8aSweepInboxRetention  = 9 * 24 * time.Hour
)

type w8aPruneStep struct {
	deleted int64
	err     error
}

type w8aPruneCall struct {
	OlderThan time.Duration
	Limit     int
}

// w8aOutboxPruner answers each call with the next scripted step and records
// the arguments of every call. beforeAnswer runs on the call's context first.
type w8aOutboxPruner struct {
	steps        []w8aPruneStep
	calls        []w8aPruneCall
	beforeAnswer func(call int)
}

func (p *w8aOutboxPruner) PrunePublished(_ context.Context, olderThan time.Duration, limit int) (int64, error) {
	p.calls = append(p.calls, w8aPruneCall{OlderThan: olderThan, Limit: limit})
	if p.beforeAnswer != nil {
		p.beforeAnswer(len(p.calls))
	}
	if len(p.calls) > len(p.steps) {
		return 0, nil
	}
	step := p.steps[len(p.calls)-1]
	return step.deleted, step.err
}

type w8aInboxCall struct {
	OlderThan time.Duration
	Batch     int
}

type w8aInboxPruner struct {
	step  w8aPruneStep
	calls []w8aInboxCall
}

func (p *w8aInboxPruner) PruneBatch(_ context.Context, olderThan time.Duration, batch int) (int64, error) {
	p.calls = append(p.calls, w8aInboxCall{OlderThan: olderThan, Batch: batch})
	return p.step.deleted, p.step.err
}

type w8aRecorded struct {
	Table   string
	Deleted int64
}

func w8aNewSweeper(outbox *w8aOutboxPruner, inbox *w8aInboxPruner, recorded *[]w8aRecorded) *service.RetentionSweeper {
	return service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: outbox, ProcessedEvents: inbox,
		OutboxRetention: w8aSweepOutboxRetention, ProcessedEventRetention: w8aSweepInboxRetention,
		Batch: w8aSweepBatch, Interval: time.Hour, Logger: &fakeLogger{},
		Record: func(table string, deleted int64) { *recorded = append(*recorded, w8aRecorded{table, deleted}) },
	})
}

func TestRetentionSweeper_OutboxPruneFailsOnTheSecondBatch_KeepsTheFirstBatchAndStops(t *testing.T) {
	outbox := &w8aOutboxPruner{steps: []w8aPruneStep{{deleted: w8aSweepBatch}, {err: errW8aOutboxPrune}}}
	inbox := &w8aInboxPruner{}
	var recorded []w8aRecorded
	sweeper := w8aNewSweeper(outbox, inbox, &recorded)

	err := sweeper.Tick(context.Background())

	require.ErrorIs(t, err, errW8aOutboxPrune)
	assert.NotErrorIs(t, err, errW8aInboxPrune)
	assert.Equal(t, []w8aPruneCall{
		{OlderThan: w8aSweepOutboxRetention, Limit: w8aSweepBatch},
		{OlderThan: w8aSweepOutboxRetention, Limit: w8aSweepBatch},
	}, outbox.calls, "the sweep stops at the failed batch instead of trying a third")
	assert.Equal(t, []w8aRecorded{
		{Table: "outbox_events", Deleted: w8aSweepBatch},
		{Table: "processed_events", Deleted: 0},
	}, recorded, "the first batch is recorded, the failed one is not, and the other table still ran")
	assert.Equal(t, []w8aInboxCall{{OlderThan: w8aSweepInboxRetention, Batch: w8aSweepBatch}}, inbox.calls)
}

func TestRetentionSweeper_BothPrunesFail_ReturnsOneErrorCarryingBothCauses(t *testing.T) {
	outbox := &w8aOutboxPruner{steps: []w8aPruneStep{{err: errW8aOutboxPrune}}}
	inbox := &w8aInboxPruner{step: w8aPruneStep{err: errW8aInboxPrune}}
	var recorded []w8aRecorded
	sweeper := w8aNewSweeper(outbox, inbox, &recorded)

	err := sweeper.Tick(context.Background())

	require.ErrorIs(t, err, errW8aOutboxPrune)
	require.ErrorIs(t, err, errW8aInboxPrune)
	var joined interface{ Unwrap() []error }
	require.ErrorAs(t, err, &joined, "the two failures come back as one joined error")
	assert.Len(t, joined.Unwrap(), 2)
	assert.Len(t, outbox.calls, 1)
	assert.Len(t, inbox.calls, 1, "the processed-events prune is not skipped by the outbox failure")
}

func TestRetentionSweeper_ContextCancelledAfterAFullOutboxBatch_StopsTheOutboxSweepButStillTriesTheOtherTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox := &w8aOutboxPruner{
		steps:        []w8aPruneStep{{deleted: w8aSweepBatch}, {deleted: w8aSweepBatch}},
		beforeAnswer: func(int) { cancel() },
	}
	inbox := &w8aInboxPruner{}
	var recorded []w8aRecorded
	sweeper := w8aNewSweeper(outbox, inbox, &recorded)

	err := sweeper.Tick(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, outbox.calls, 1, "no further batch is pruned once the context is done")
	assert.Equal(t, []w8aRecorded{
		{Table: "outbox_events", Deleted: w8aSweepBatch},
		{Table: "processed_events", Deleted: 0},
	}, recorded, "the full batch was already deleted, so it is counted")
	require.Len(t, inbox.calls, 1, "the processed-events prune is still attempted")
}
