//go:build integration

package valkeystream_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

// A key that holds something other than a stream makes every stream command on it fail; the wrappers name the
// step that failed so an operator can tell a trim failure from a dead-letter failure.
func TestConsumer_KeyThatIsNotAStream_EachCallNamesTheFailingStep(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	t.Run("trim to the oldest pending entry fails reading the group", func(t *testing.T) {
		key := vsxStream(t)
		require.NoError(t, client.Set(ctx, key, "not a stream", 0).Err())

		trimmed, err := consumer.TrimToOldestPending(ctx, key, vsxGroup)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "valkeystream trim: group info: ")
		assert.Zero(t, trimmed)
	})

	t.Run("trim by age fails and trims nothing", func(t *testing.T) {
		key := vsxStream(t)
		require.NoError(t, client.Set(ctx, key, "not a stream", 0).Err())

		trimmed, err := consumer.TrimOlderThan(ctx, key, time.Now())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "valkeystream trim by age: ")
		assert.Zero(t, trimmed)
		value, getErr := client.Get(ctx, key).Result()
		require.NoError(t, getErr)
		assert.Equal(t, "not a stream", value, "the key is left as it was")
	})

	t.Run("dead letter whose ack fails keeps the copy and names the ack", func(t *testing.T) {
		source := vsxStream(t)
		deadKey := source + "-dead"
		require.NoError(t, client.Set(ctx, source, "not a stream", 0).Err())
		entry := valkeystream.Entry{ID: "1-0", Fields: map[string]string{"task_id": "t-1"}}

		err := consumer.DeadLetter(ctx, source, vsxGroup, deadKey, entry, "malformed_entry")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "valkeystream dead letter: ack: ")
		copies, rangeErr := client.XRange(ctx, deadKey, "-", "+").Result()
		require.NoError(t, rangeErr)
		require.Len(t, copies, 1, "the copy is written before the ack")
		assert.Equal(t, map[string]any{"task_id": "t-1", "reason": "malformed_entry", "source_entry_id": "1-0"}, copies[0].Values)
	})
}
