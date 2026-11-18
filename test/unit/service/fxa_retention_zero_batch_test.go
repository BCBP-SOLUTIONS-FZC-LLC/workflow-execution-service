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

type fxaCountingOutboxPruner struct {
	limits  []int
	maxCall int
}

func (p *fxaCountingOutboxPruner) PrunePublished(_ context.Context, _ time.Duration, limit int) (int64, error) {
	p.limits = append(p.limits, limit)
	if len(p.limits) > p.maxCall {
		return 0, errors.New("pruner called again: the sweep does not stop")
	}
	return 0, nil
}

type fxaCountingInboxPruner struct{ batches []int }

func (p *fxaCountingInboxPruner) PruneBatch(_ context.Context, _ time.Duration, batch int) (int64, error) {
	p.batches = append(p.batches, batch)
	return 0, nil
}

func TestRetentionSweeper_AZeroBatchPrunesEachTableOnceAndStops(t *testing.T) {
	outbox := &fxaCountingOutboxPruner{maxCall: 3}
	inbox := &fxaCountingInboxPruner{}
	recorded := map[string][]int64{}
	sweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: outbox, ProcessedEvents: inbox,
		OutboxRetention: time.Hour, ProcessedEventRetention: time.Hour,
		Batch: 0, Interval: time.Hour, Logger: &fakeLogger{},
		Record: func(table string, deleted int64) { recorded[table] = append(recorded[table], deleted) },
	})

	err := sweeper.Tick(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []int{0}, outbox.limits, "the outbox is pruned once")
	assert.Equal(t, []int{0}, inbox.batches, "the processed events are pruned once")
	assert.Equal(t, []int64{0}, recorded["outbox_events"])
	assert.Equal(t, []int64{0}, recorded["processed_events"])
}
