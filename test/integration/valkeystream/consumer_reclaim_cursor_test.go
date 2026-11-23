//go:build integration

package valkeystream_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

// Valkey scans only ten pending entries per claimed entry asked for, so a stale entry behind a longer run
// of busy ones is reached only by a caller that resumes from the cursor of the previous call.
func TestConsumerGroup_ReclaimStale_ResumingFromTheCursorReachesAStaleEntryBehindBusyOnes(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)
	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))
	const busy, total = 20, 21
	for i := 0; i < total; i++ {
		_, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
	}
	entries, err := consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", total, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, total)
	time.Sleep(1200 * time.Millisecond)
	busyIDs := make([]string, 0, busy)
	for _, e := range entries[:busy] {
		busyIDs = append(busyIDs, e.ID)
	}
	require.NoError(t, consumer.Touch(ctx, stream, testGroup, "consumer-a", busyIDs))

	var reclaimed []valkeystream.Entry
	cursor := "0"
	for call := 0; call < 5 && len(reclaimed) == 0; call++ {
		reclaimed, cursor, err = consumer.ReclaimStale(ctx, stream, testGroup, "consumer-b", cursor, time.Second, 1)
		require.NoError(t, err)
	}

	require.Len(t, reclaimed, 1, "the stale entry is reached within a few calls")
	assert.Equal(t, entries[total-1].ID, reclaimed[0].ID)
}

func TestConsumerGroup_ReclaimStale_AnExhaustedListAnswersTheStartCursor(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))

	reclaimed, cursor, err := consumer.ReclaimStale(ctx, stream, testGroup, "consumer-b", "0", time.Millisecond, 10)

	require.NoError(t, err)
	assert.Empty(t, reclaimed)
	assert.Equal(t, "0-0", cursor)
}
