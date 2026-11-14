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

var errPartialPruneConsumer = errors.New("consumer orders: prune failed")

type partialPruneInbox struct{}

func (partialPruneInbox) PruneBatch(context.Context, time.Duration, int) (int64, error) {
	return 7, errPartialPruneConsumer
}

// When one consumer's prune fails, the rows the other consumers deleted are still counted, and the failure is
// returned naming the processed-events prune.
func TestRetentionSweeper_ProcessedEventsPartlyPruned_CountsWhatWasDeletedAndReturnsTheError(t *testing.T) {
	recorded := map[string]int64{}
	sweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox: &fakeOutboxPruner{}, ProcessedEvents: partialPruneInbox{},
		OutboxRetention: time.Hour, ProcessedEventRetention: time.Hour,
		Batch: 100, Interval: time.Hour, Logger: &fakeLogger{},
		Record: func(table string, n int64) { recorded[table] += n },
	})

	err := sweeper.Tick(context.Background())

	require.ErrorIs(t, err, errPartialPruneConsumer)
	assert.Contains(t, err.Error(), "prune processed events")
	assert.Equal(t, int64(7), recorded["processed_events"])
}
