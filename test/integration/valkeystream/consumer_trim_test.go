//go:build integration

package valkeystream_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

func vsxEntryIDs(t *testing.T, client *redis.Client, stream string) []string {
	t.Helper()
	messages, err := client.XRange(context.Background(), stream, "-", "+").Result()
	require.NoError(t, err)
	ids := make([]string, len(messages))
	for i, m := range messages {
		ids[i] = m.ID
	}
	return ids
}

func TestConsumer_TrimToOldestPending(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	t.Run("drops acked entries before the oldest pending one and keeps everything from it on", func(t *testing.T) {
		stream := vsxStream(t)
		entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, 5)
		require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, entries[0].ID))
		require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, entries[1].ID))
		require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, entries[3].ID))
		require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, entries[4].ID))

		trimmed, err := consumer.TrimToOldestPending(ctx, stream, vsxGroup)

		require.NoError(t, err)
		assert.EqualValues(t, 2, trimmed)
		assert.Equal(t, []string{entries[2].ID, entries[3].ID, entries[4].ID}, vsxEntryIDs(t, client, stream))
		vsxMakeIdle(t, client, stream, vsxClaimerA, []string{entries[2].ID}, vsxLongIdle)
		reclaimed, err := consumer.ReclaimStale(ctx, stream, vsxGroup, vsxClaimerB, vsxMinIdle, vsxReadBatch)
		require.NoError(t, err)
		require.Len(t, reclaimed, 1)
		assert.Equal(t, entries[2].ID, reclaimed[0].ID, "the pending entry survived the trim with its fields")
		assert.Equal(t, "2", reclaimed[0].Fields["seq"])
	})

	t.Run("never drops an entry pending under another consumer", func(t *testing.T) {
		stream := vsxStream(t)
		entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, 3)
		vsxMakeIdle(t, client, stream, vsxClaimerB, []string{entries[0].ID}, 0)
		require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, entries[1].ID))
		require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, entries[2].ID))

		trimmed, err := consumer.TrimToOldestPending(ctx, stream, vsxGroup)

		require.NoError(t, err)
		assert.Zero(t, trimmed)
		assert.Equal(t, []string{entries[0].ID, entries[1].ID, entries[2].ID}, vsxEntryIDs(t, client, stream))
	})

	t.Run("with nothing pending keeps the entries not yet delivered", func(t *testing.T) {
		stream := vsxStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, vsxGroup))
		producer := valkeystream.NewProducer(client)
		ids := make([]string, 5)
		for i := range ids {
			id, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprint(i)})
			require.NoError(t, err)
			ids[i] = id
		}
		delivered, err := consumer.ReadGroup(ctx, stream, vsxGroup, vsxClaimerA, 3, time.Second)
		require.NoError(t, err)
		require.Len(t, delivered, 3)
		for _, e := range delivered {
			require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, e.ID))
		}

		_, err = consumer.TrimToOldestPending(ctx, stream, vsxGroup)

		require.NoError(t, err)
		remaining := vsxEntryIDs(t, client, stream)
		assert.Contains(t, remaining, ids[3])
		assert.Contains(t, remaining, ids[4])
		assert.NotContains(t, remaining, ids[0])
		next, err := consumer.ReadGroup(ctx, stream, vsxGroup, vsxClaimerA, 10, time.Second)
		require.NoError(t, err)
		require.Len(t, next, 2, "both undelivered entries are still readable")
	})

	t.Run("a group that does not exist is an error naming the trim", func(t *testing.T) {
		stream := vsxStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, vsxGroup))

		_, err := consumer.TrimToOldestPending(ctx, stream, "vsx-other-group")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "valkeystream trim")
	})
}

func TestConsumer_TrimOlderThan_DropsOnlyEntriesBeforeTheCutoff(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)
	stream := vsxStream(t)
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-30 * 24 * time.Hour)
	ages := []time.Duration{-31 * 24 * time.Hour, -40 * 24 * time.Hour, -29 * 24 * time.Hour, -time.Hour}
	var ids []string
	for _, age := range ages {
		ids = append(ids, fmt.Sprintf("%d-0", now.Add(age).UnixMilli()))
	}
	for _, id := range []string{ids[1], ids[0], ids[2], ids[3]} {
		require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: id, Values: map[string]any{"k": "v"}}).Err())
	}

	trimmed, err := consumer.TrimOlderThan(ctx, stream, cutoff)

	require.NoError(t, err)
	assert.EqualValues(t, 2, trimmed)
	assert.Equal(t, []string{ids[2], ids[3]}, vsxEntryIDs(t, client, stream))
}

func TestConsumer_TrimOlderThan_EmptyOrMissingStream_TrimsNothing(t *testing.T) {
	client := vsxNewClient(t)

	trimmed, err := valkeystream.NewConsumer(client).TrimOlderThan(context.Background(), vsxStream(t), time.Now())

	require.NoError(t, err)
	assert.Zero(t, trimmed)
}
