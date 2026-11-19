//go:build integration

package valkeystream_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

const (
	w30bGroup      = "w30b-group"
	w30bOtherGroup = "w30b-other-group"
	w30bReaderA    = "w30b-consumer-a"
	w30bReaderB    = "w30b-consumer-b"
)

func w30bNewClient(t *testing.T) *redis.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("w30bNewClient: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379")
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: host + ":" + port.Port()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func w30bStream(t *testing.T) string {
	return "w30b-" + strings.ReplaceAll(t.Name(), "/", "-")
}

func w30bPublish(t *testing.T, client *redis.Client, stream string, n int) []string {
	t.Helper()
	producer := valkeystream.NewProducer(client)
	ids := make([]string, n)
	for i := range n {
		id, err := producer.Publish(context.Background(), stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
		ids[i] = id
	}
	return ids
}

// w30bFailCommand makes one Valkey command fail on the client it is added to; every other command goes through.
type w30bFailCommand struct {
	name string
	err  error
}

func (f w30bFailCommand) DialHook(next redis.DialHook) redis.DialHook { return next }
func (f w30bFailCommand) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (f w30bFailCommand) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == f.name {
			cmd.SetErr(f.err)
			return f.err
		}
		return next(ctx, cmd)
	}
}

// w30bSameServer returns a second client on the same server, so a hook can be added without touching the first.
func w30bSameServer(t *testing.T, client *redis.Client) *redis.Client {
	t.Helper()
	other := redis.NewClient(&redis.Options{Addr: client.Options().Addr})
	t.Cleanup(func() { _ = other.Close() })
	return other
}

func TestConsumer_EnsureGroup_Stream_CreatedEmptyAndGroupStartsAtZero(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	t.Run("missing stream is created empty with the group at id 0", func(t *testing.T) {
		stream := w30bStream(t)

		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))

		length, err := client.XLen(ctx, stream).Result()
		require.NoError(t, err)
		assert.Zero(t, length, "MKSTREAM creates the stream without entries")
		groups, err := client.XInfoGroups(ctx, stream).Result()
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, w30bGroup, groups[0].Name)
		assert.Equal(t, "0-0", groups[0].LastDeliveredID)
	})

	t.Run("entries published before the group exists are still delivered", func(t *testing.T) {
		stream := w30bStream(t)
		ids := w30bPublish(t, client, stream, 3)

		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))

		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 10, time.Second)
		require.NoError(t, err)
		got := make([]string, len(entries))
		for i, e := range entries {
			got[i] = e.ID
		}
		assert.Equal(t, ids, got, "a group created at 0 sees what was published before it")
	})
}

func TestConsumer_ReadGroup_BlockExpires_ReturnsNilEntriesAndNoError(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	t.Run("empty stream", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))

		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 10, 50*time.Millisecond)

		require.NoError(t, err)
		assert.Nil(t, entries)
	})

	t.Run("drained stream", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
		w30bPublish(t, client, stream, 2)
		first, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 10, time.Second)
		require.NoError(t, err)
		require.Len(t, first, 2)

		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderB, 10, 50*time.Millisecond)

		require.NoError(t, err)
		assert.Nil(t, entries)
	})
}

func TestConsumer_ReadGroup_TwoConsumersInTurn_SplitTheEntriesWithoutOverlap(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()
	stream := w30bStream(t)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
	published := w30bPublish(t, client, stream, 10)

	first, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 4, time.Second)
	require.NoError(t, err)
	second, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderB, 4, time.Second)
	require.NoError(t, err)
	third, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 4, time.Second)
	require.NoError(t, err)

	require.Len(t, first, 4)
	require.Len(t, second, 4)
	require.Len(t, third, 2)
	var all []string
	for _, batch := range [][]valkeystream.Entry{first, second, third} {
		for _, e := range batch {
			all = append(all, e.ID)
		}
	}
	assert.Equal(t, published, all, "each entry went to exactly one consumer, in stream order")
	summary, err := client.XPending(ctx, stream, w30bGroup).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{w30bReaderA: 6, w30bReaderB: 4}, summary.Consumers)
}

func TestConsumer_Ack_IdThatIsNotPending_ReturnsNil(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	t.Run("already acked", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
		w30bPublish(t, client, stream, 1)
		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 1, time.Second)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.NoError(t, consumer.Ack(ctx, stream, w30bGroup, entries[0].ID))

		require.NoError(t, consumer.Ack(ctx, stream, w30bGroup, entries[0].ID))
	})

	t.Run("never delivered", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
		ids := w30bPublish(t, client, stream, 1)

		require.NoError(t, consumer.Ack(ctx, stream, w30bGroup, ids[0]))
		require.NoError(t, consumer.Ack(ctx, stream, w30bGroup, "999999999999999-0"))

		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 1, time.Second)
		require.NoError(t, err)
		require.Len(t, entries, 1, "acking an undelivered id does not remove it from the group's backlog")
		assert.Equal(t, ids[0], entries[0].ID)
	})

	t.Run("from another group", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bOtherGroup))
		w30bPublish(t, client, stream, 1)
		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 1, time.Second)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		require.NoError(t, consumer.Ack(ctx, stream, w30bOtherGroup, entries[0].ID))

		summary, err := client.XPending(ctx, stream, w30bGroup).Result()
		require.NoError(t, err)
		assert.EqualValues(t, 1, summary.Count, "the other group's ack leaves this group's entry pending")
	})

	t.Run("malformed id is refused by the server and wrapped", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))

		err := consumer.Ack(ctx, stream, w30bGroup, "not-an-id")

		require.Error(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "valkeystream ack: "), err.Error())
		var serverErr redis.Error
		assert.ErrorAs(t, err, &serverErr)
	})
}

func TestConsumer_TrimToOldestPending_FailingStep_NamesItAndTrimsNothing(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()

	setup := func(t *testing.T) (string, []string) {
		stream := w30bStream(t)
		require.NoError(t, valkeystream.NewConsumer(client).EnsureGroup(ctx, stream, w30bGroup))
		ids := w30bPublish(t, client, stream, 3)
		entries, err := valkeystream.NewConsumer(client).ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 3, time.Second)
		require.NoError(t, err)
		require.Len(t, entries, 3)
		for _, e := range entries {
			require.NoError(t, valkeystream.NewConsumer(client).Ack(ctx, stream, w30bGroup, e.ID))
		}
		return stream, ids
	}

	t.Run("pending summary read fails", func(t *testing.T) {
		stream, ids := setup(t)
		cause := errors.New("w30b xpending unavailable")
		failing := w30bSameServer(t, client)
		failing.AddHook(w30bFailCommand{name: "xpending", err: cause})

		trimmed, err := valkeystream.NewConsumer(failing).TrimToOldestPending(ctx, stream, w30bGroup)

		require.ErrorIs(t, err, cause)
		assert.Contains(t, err.Error(), "valkeystream trim: pending summary: ")
		assert.Zero(t, trimmed)
		remaining, xerr := client.XRange(ctx, stream, "-", "+").Result()
		require.NoError(t, xerr)
		require.Len(t, remaining, len(ids), "nothing is trimmed when the bound cannot be read")
	})

	t.Run("trim command fails", func(t *testing.T) {
		stream, ids := setup(t)
		cause := errors.New("w30b xtrim unavailable")
		failing := w30bSameServer(t, client)
		failing.AddHook(w30bFailCommand{name: "xtrim", err: cause})

		trimmed, err := valkeystream.NewConsumer(failing).TrimToOldestPending(ctx, stream, w30bGroup)

		require.ErrorIs(t, err, cause)
		assert.True(t, strings.HasPrefix(err.Error(), "valkeystream trim: "), err.Error())
		assert.NotContains(t, err.Error(), "pending summary", "the failing step is the trim itself")
		assert.Zero(t, trimmed)
		remaining, xerr := client.XRange(ctx, stream, "-", "+").Result()
		require.NoError(t, xerr)
		require.Len(t, remaining, len(ids))
	})

	t.Run("group missing", func(t *testing.T) {
		stream := w30bStream(t)
		w30bPublish(t, client, stream, 2)

		trimmed, err := valkeystream.NewConsumer(client).TrimToOldestPending(ctx, stream, w30bGroup)

		require.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("valkeystream trim: group %q does not exist on %q", w30bGroup, stream))
		assert.Zero(t, trimmed)
		length, xerr := client.XLen(ctx, stream).Result()
		require.NoError(t, xerr)
		assert.EqualValues(t, 2, length)
	})
}

func TestConsumer_ServerRefusals_KeepTheServerErrorReachable(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	t.Run("ensure group on a key that holds a string", func(t *testing.T) {
		key := w30bStream(t)
		require.NoError(t, client.Set(ctx, key, "not a stream", 0).Err())

		err := consumer.EnsureGroup(ctx, key, w30bGroup)

		require.Error(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "valkeystream ensure group: "), err.Error())
		assert.Contains(t, err.Error(), "WRONGTYPE")
		var serverErr redis.Error
		assert.ErrorAs(t, err, &serverErr)
	})

	t.Run("read group when the group does not exist", func(t *testing.T) {
		stream := w30bStream(t)
		w30bPublish(t, client, stream, 1)

		entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 10, 50*time.Millisecond)

		require.Error(t, err)
		assert.Nil(t, entries)
		assert.True(t, strings.HasPrefix(err.Error(), "valkeystream read group: "), err.Error())
		assert.Contains(t, err.Error(), "NOGROUP")
		var serverErr redis.Error
		assert.ErrorAs(t, err, &serverErr)
		assert.False(t, errors.Is(err, redis.Nil), "a missing group is not an empty read")
	})

	t.Run("read group after the stream key was deleted", func(t *testing.T) {
		stream := w30bStream(t)
		require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
		require.NoError(t, client.Del(ctx, stream).Err())

		_, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 10, 50*time.Millisecond)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "NOGROUP")
	})

	t.Run("reclaim stale when the group does not exist", func(t *testing.T) {
		stream := w30bStream(t)
		w30bPublish(t, client, stream, 1)

		entries, err := consumer.ReclaimStale(ctx, stream, w30bGroup, w30bReaderB, time.Millisecond, 10)

		require.Error(t, err)
		assert.Nil(t, entries)
		assert.True(t, strings.HasPrefix(err.Error(), "valkeystream reclaim stale: "), err.Error())
		assert.Contains(t, err.Error(), "NOGROUP")
		var serverErr redis.Error
		assert.ErrorAs(t, err, &serverErr)
	})
}

func TestConsumer_Ack_ServerUnreachable_EntryStaysPendingAndIsReclaimedAsSecondDelivery(t *testing.T) {
	client := w30bNewClient(t)
	ctx := context.Background()
	stream := w30bStream(t)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, w30bGroup))
	w30bPublish(t, client, stream, 1)
	entries, err := consumer.ReadGroup(ctx, stream, w30bGroup, w30bReaderA, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.EqualValues(t, 1, entries[0].Deliveries)

	unreachable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = unreachable.Close() })
	ackErr := valkeystream.NewConsumer(unreachable).Ack(ctx, stream, w30bGroup, entries[0].ID)

	require.Error(t, ackErr)
	assert.True(t, strings.HasPrefix(ackErr.Error(), "valkeystream ack: "), ackErr.Error())
	var netErr net.Error
	assert.ErrorAs(t, ackErr, &netErr)
	summary, err := client.XPending(ctx, stream, w30bGroup).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, summary.Count, "an ack that never reached Valkey leaves the entry pending")
	require.NoError(t, client.Do(ctx, "XCLAIM", stream, w30bGroup, w30bReaderA, 0, entries[0].ID, "IDLE", time.Hour.Milliseconds(), "JUSTID").Err())
	reclaimed, err := consumer.ReclaimStale(ctx, stream, w30bGroup, w30bReaderB, 30*time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	assert.Equal(t, entries[0].ID, reclaimed[0].ID)
	assert.EqualValues(t, 2, reclaimed[0].Deliveries)
}
