package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type strfixBatchInbox struct {
	batches   []int
	olderThan time.Duration
}

func (f *strfixBatchInbox) PruneBatch(_ context.Context, olderThan time.Duration, batch int) (int64, error) {
	f.olderThan = olderThan
	f.batches = append(f.batches, batch)
	return 0, nil
}

func TestRetentionSweeper_ProcessedEventsPruneUsesTheSweepBatch(t *testing.T) {
	inbox := &strfixBatchInbox{}
	sweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: &fakeOutboxPruner{}, ProcessedEvents: inbox,
		OutboxRetention: time.Hour, ProcessedEventRetention: 9 * 24 * time.Hour,
		Batch: 250, Interval: time.Hour, Record: func(string, int64) {}, Logger: &fakeLogger{},
	})

	require.NoError(t, sweeper.Tick(context.Background()))

	assert.Equal(t, []int{250}, inbox.batches)
	assert.Equal(t, 9*24*time.Hour, inbox.olderThan)
}
