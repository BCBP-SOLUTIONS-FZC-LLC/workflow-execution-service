//go:build integration

package valkeystream_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	vsxGroup     = "vsx-group"
	vsxLongIdle  = time.Hour
	vsxMinIdle   = 30 * time.Minute
	vsxClaimerA  = "vsx-consumer-a"
	vsxClaimerB  = "vsx-consumer-b"
	vsxClaimerC  = "vsx-consumer-c"
	vsxReadBatch = 100
)

func vsxNewClient(t *testing.T) *redis.Client {
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
		t.Fatalf("vsxNewClient: start container: %v", err)
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

// vsxStream gives each subtest its own stream key so subtests never see each other's entries.
func vsxStream(t *testing.T) string {
	return "vsx-" + strings.ReplaceAll(t.Name(), "/", "-")
}

// vsxPublishAndRead publishes n entries and delivers all of them to consumerName.
func vsxPublishAndRead(t *testing.T, client *redis.Client, stream, consumerName string, n int) []valkeystream.Entry {
	t.Helper()
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, vsxGroup))
	producer := valkeystream.NewProducer(client)
	for i := range n {
		_, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
	}
	entries, err := consumer.ReadGroup(ctx, stream, vsxGroup, consumerName, int64(n), time.Second)
	require.NoError(t, err)
	require.Len(t, entries, n)
	return entries
}

// vsxMakeIdle sets the entry's idle time without waiting, so reclaim tests do not sleep.
func vsxMakeIdle(t *testing.T, client *redis.Client, stream, owner string, ids []string, idle time.Duration) {
	t.Helper()
	for _, id := range ids {
		args := []any{"XCLAIM", stream, vsxGroup, owner, 0, id, "IDLE", idle.Milliseconds(), "JUSTID"}
		require.NoError(t, client.Do(context.Background(), args...).Err())
	}
}

func vsxPending(t *testing.T, client *redis.Client, stream string) []redis.XPendingExt {
	t.Helper()
	pending, err := client.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: vsxGroup, Start: "-", End: "+", Count: 1000,
	}).Result()
	require.NoError(t, err)
	return pending
}

func TestConsumer_EnsureGroup_SecondCall_KeepsGroupPositionAndPending(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	consumer := valkeystream.NewConsumer(client)
	producer := valkeystream.NewProducer(client)

	require.NoError(t, consumer.EnsureGroup(ctx, stream, vsxGroup))
	for i := range 2 {
		_, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
	}
	first, err := consumer.ReadGroup(ctx, stream, vsxGroup, vsxClaimerA, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, first, 1)

	require.NoError(t, consumer.EnsureGroup(ctx, stream, vsxGroup), "an existing group is not an error")

	groups, err := client.XInfoGroups(ctx, stream).Result()
	require.NoError(t, err)
	require.Len(t, groups, 1, "the second call must not create a second group")
	assert.Equal(t, vsxGroup, groups[0].Name)
	assert.Equal(t, int64(1), groups[0].Pending, "the delivered entry is still pending")
	assert.Equal(t, first[0].ID, groups[0].LastDeliveredID, "the group position was not reset")

	next, err := consumer.ReadGroup(ctx, stream, vsxGroup, vsxClaimerA, 10, time.Second)
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.Equal(t, "1", next[0].Fields["seq"], "reading resumes after the first entry instead of replaying from the start")
	assert.NotEqual(t, first[0].ID, next[0].ID)
}

func TestConsumer_ReclaimStale_PendingEntryDeletedFromStream_IsNotReturned(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	consumer := valkeystream.NewConsumer(client)

	entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, 2)
	deleted, kept := entries[0], entries[1]
	require.NoError(t, client.XDel(ctx, stream, deleted.ID).Err())
	vsxMakeIdle(t, client, stream, vsxClaimerA, []string{deleted.ID, kept.ID}, vsxLongIdle)

	reclaimed, err := consumer.ReclaimStale(ctx, stream, vsxGroup, vsxClaimerB, vsxMinIdle, vsxReadBatch)

	require.NoError(t, err)
	require.Len(t, reclaimed, 1, "only the entry that still exists in the stream comes back")
	assert.Equal(t, kept.ID, reclaimed[0].ID)
	assert.Equal(t, "1", reclaimed[0].Fields["seq"])
	for _, p := range vsxPending(t, client, stream) {
		assert.NotEqual(t, deleted.ID, p.ID, "the deleted entry must not stay claimable")
	}
}

func TestConsumer_Touch_IdsThatCannotBeClaimed_ReturnsNilAndClaimsNothing(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	consumer := valkeystream.NewConsumer(client)
	producer := valkeystream.NewProducer(client)

	require.NoError(t, consumer.EnsureGroup(ctx, stream, vsxGroup))
	ids := make([]string, 4)
	for i := range ids {
		id, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
		ids[i] = id
	}
	acked, deleted, live, neverDelivered := ids[0], ids[1], ids[2], ids[3]
	delivered, err := consumer.ReadGroup(ctx, stream, vsxGroup, vsxClaimerA, 3, time.Second)
	require.NoError(t, err)
	require.Len(t, delivered, 3)
	require.NoError(t, consumer.Ack(ctx, stream, vsxGroup, acked))
	require.NoError(t, client.XDel(ctx, stream, deleted).Err())

	err = consumer.Touch(ctx, stream, vsxGroup, vsxClaimerB, []string{acked, deleted, neverDelivered})

	require.NoError(t, err)
	for _, p := range vsxPending(t, client, stream) {
		assert.NotEqual(t, vsxClaimerB, p.Consumer, "touching claimed entry %s for the toucher", p.ID)
		assert.NotEqual(t, acked, p.ID, "an acked entry must not come back into the pending list")
		assert.NotEqual(t, neverDelivered, p.ID, "an undelivered entry must not enter the pending list")
	}
	var liveOwner string
	for _, p := range vsxPending(t, client, stream) {
		if p.ID == live {
			liveOwner = p.Consumer
		}
	}
	assert.Equal(t, vsxClaimerA, liveOwner, "an entry that was not touched keeps its owner")

	next, err := consumer.ReadGroup(ctx, stream, vsxGroup, vsxClaimerB, 10, time.Second)
	require.NoError(t, err)
	require.Len(t, next, 1, "the never-delivered entry is still waiting to be read")
	assert.Equal(t, neverDelivered, next[0].ID)
}

func TestConsumer_DeadLetter_EntryWithoutFields_HoldsOnlyReasonAndSourceID(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	consumer := valkeystream.NewConsumer(client)

	cases := []struct {
		name   string
		fields map[string]string
	}{
		{"nil fields", nil},
		{"empty fields", map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := vsxStream(t)
			deadStream := stream + "-dead"
			delivered := vsxPublishAndRead(t, client, stream, vsxClaimerA, 1)

			err := consumer.DeadLetter(ctx, stream, vsxGroup, deadStream,
				valkeystream.Entry{ID: delivered[0].ID, Fields: tc.fields}, "vsx-reason")

			require.NoError(t, err)
			dead, err := client.XRange(ctx, deadStream, "-", "+").Result()
			require.NoError(t, err)
			require.Len(t, dead, 1)
			assert.Equal(t, map[string]any{"reason": "vsx-reason", "source_entry_id": delivered[0].ID}, dead[0].Values)
			assert.Empty(t, vsxPending(t, client, stream), "the source entry is acknowledged")
		})
	}
}

func TestConsumer_ReclaimStale_ConcurrentCallers_EachEntryGoesToExactlyOneCaller(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	consumer := valkeystream.NewConsumer(client)

	const total = 30
	entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, total)
	ids := make([]string, total)
	for i, e := range entries {
		ids[i] = e.ID
	}
	vsxMakeIdle(t, client, stream, vsxClaimerA, ids, vsxLongIdle)

	claimers := []string{vsxClaimerB, vsxClaimerC}
	got := make([][]valkeystream.Entry, len(claimers))
	errs := make([]error, len(claimers))
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	for i, name := range claimers {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			for {
				batch, err := consumer.ReclaimStale(ctx, stream, vsxGroup, name, vsxMinIdle, 4)
				if err != nil {
					errs[i] = err
					return
				}
				if len(batch) == 0 {
					return
				}
				got[i] = append(got[i], batch...)
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	owner := make(map[string]string, total)
	for i, batch := range got {
		for _, e := range batch {
			prev, dup := owner[e.ID]
			require.False(t, dup, "entry %s was handed to both %s and %s", e.ID, prev, claimers[i])
			owner[e.ID] = claimers[i]
			assert.Equal(t, int64(2), e.Deliveries, "entry %s: one read plus one reclaim", e.ID)
		}
	}
	assert.Len(t, owner, total, "every stale entry is reclaimed by someone")
	for _, p := range vsxPending(t, client, stream) {
		assert.Equal(t, owner[p.ID], p.Consumer, "entry %s is owned by the caller that received it", p.ID)
	}
}

func TestProducer_Publish_NoFields_ReturnsXaddErrorAndCreatesNothing(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	producer := valkeystream.NewProducer(client)

	cases := []struct {
		name   string
		fields map[string]string
	}{
		{"nil fields", nil},
		{"empty fields", map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := vsxStream(t)

			id, err := producer.Publish(ctx, stream, tc.fields)

			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "valkeystream xadd: "), err.Error())
			assert.Empty(t, id)
			exists, err := client.Exists(ctx, stream).Result()
			require.NoError(t, err)
			assert.Zero(t, exists, "a rejected publish must not create the stream")
		})
	}
}
