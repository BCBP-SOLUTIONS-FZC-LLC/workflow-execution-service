//go:build integration

package valkeystream_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

func TestConsumer_DeadLetter_CopyFails_EntryStaysPendingAndIsReclaimable(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	deadKey := stream + "-dead"
	entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, 1)
	require.NoError(t, client.Set(ctx, deadKey, "not a stream", 0).Err())
	consumer := valkeystream.NewConsumer(client)

	err := consumer.DeadLetter(ctx, stream, vsxGroup, deadKey, entries[0], "max_deliveries_exceeded")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkeystream dead letter")
	pending := vsxPending(t, client, stream)
	require.Len(t, pending, 1, "an entry whose dead-letter copy failed must not be acked")
	assert.Equal(t, entries[0].ID, pending[0].ID)

	vsxMakeIdle(t, client, stream, vsxClaimerA, []string{entries[0].ID}, vsxLongIdle)
	reclaimed, err := consumer.ReclaimStale(ctx, stream, vsxGroup, vsxClaimerB, vsxMinIdle, vsxReadBatch)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	assert.Equal(t, entries[0].ID, reclaimed[0].ID)
}

func TestConsumer_DeadLetter_CopyWritten_SourceIsAckedAfterwards(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	deadKey := stream + "-dead"
	entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, 1)
	consumer := valkeystream.NewConsumer(client)

	require.NoError(t, consumer.DeadLetter(ctx, stream, vsxGroup, deadKey, entries[0], "max_deliveries_exceeded"))

	assert.Empty(t, vsxPending(t, client, stream))
	copies, err := client.XRange(ctx, deadKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, copies, 1)
	assert.Equal(t, entries[0].ID, copies[0].Values["source_entry_id"])
}
