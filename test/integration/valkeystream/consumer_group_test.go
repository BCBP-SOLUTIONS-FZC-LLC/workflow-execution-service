//go:build integration

// Package valkeystream_test exercises internal/adapter/outbound/valkeystream
// against a real Valkey container — consumer-group ack/redelivery semantics
// (XREADGROUP/XACK/XAUTOCLAIM) are the one thing a unit test can't credibly
// fake, and getting them subtly wrong (an off-by-one on the ">" vs.
// explicit-ID argument, a shared-vs-per-goroutine Consumer bug) would show
// up as either double-delivery or a dropped entry — exactly what
// TestConsumerGroup_TwoConsumersNoDoubleDelivery below is built to catch.
package valkeystream_test

import (
	"context"
	"errors"
	"fmt"
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
	testStream = "connector-tasks-test"
	testGroup  = "connector-worker-test"
)

func newTestClient(t *testing.T) redis.Cmdable {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("valkeystream_test.newTestClient: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("valkeystream_test.newTestClient: container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("valkeystream_test.newTestClient: mapped port: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: host + ":" + port.Port()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newStreamName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%s", testStream, t.Name())
}

func TestConsumerGroup_ReadAck(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))

	const n = 5
	for i := 0; i < n; i++ {
		_, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
	}

	entries, err := consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", n, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, n)

	for _, e := range entries {
		require.NoError(t, consumer.Ack(ctx, stream, testGroup, e.ID))
	}

	pending := client.XPending(ctx, stream, testGroup)
	res, err := pending.Result()
	require.NoError(t, err)
	assert.Zero(t, res.Count)
}

func TestConsumerGroup_UnackedEntryReclaimable(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))

	_, err := producer.Publish(ctx, stream, map[string]string{"seq": "0"})
	require.NoError(t, err)

	entries, err := consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	// Deliberately never Ack — simulates consumer-a dying mid-dispatch.

	time.Sleep(50 * time.Millisecond)
	reclaimed, err := consumer.ReclaimStale(ctx, stream, testGroup, "consumer-b", 10*time.Millisecond, 10)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	assert.Equal(t, entries[0].ID, reclaimed[0].ID)
	assert.Equal(t, "0", reclaimed[0].Fields["seq"])

	// consumer-b now owns it and can Ack it.
	require.NoError(t, consumer.Ack(ctx, stream, testGroup, reclaimed[0].ID))
}

func TestConsumerGroup_TwoConsumersNoDoubleDelivery(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))

	const n = 20
	published := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		id, err := producer.Publish(ctx, stream, map[string]string{"seq": fmt.Sprintf("%d", i)})
		require.NoError(t, err)
		published[id] = true
	}

	seen := make(map[string]bool, n)
	for consumerIdx, name := range []string{"consumer-a", "consumer-b"} {
		entries, err := consumer.ReadGroup(ctx, stream, testGroup, name, n, 100*time.Millisecond)
		require.NoErrorf(t, err, "consumer %d (%s)", consumerIdx, name)
		for _, e := range entries {
			require.Falsef(t, seen[e.ID], "entry %s delivered to more than one consumer", e.ID)
			seen[e.ID] = true
			require.NoError(t, consumer.Ack(ctx, stream, testGroup, e.ID))
		}
	}

	assert.Len(t, seen, n, "every published entry must be delivered exactly once across both consumers")
	for id := range published {
		assert.True(t, seen[id], "entry %s was never delivered to either consumer", id)
	}
}

func TestConsumerGroup_EnsureGroupIdempotent(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)

	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup), "a second EnsureGroup call must tolerate BUSYGROUP, not error")
}

// The delivery cap reads this count, so a reclaim must report every delivery
// the group has made, not just that the entry came back.
func TestConsumerGroup_ReclaimStale_ReportsDeliveries(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))
	_, err := producer.Publish(ctx, stream, map[string]string{"seq": "0"})
	require.NoError(t, err)

	entries, err := consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, int64(1), entries[0].Deliveries)

	for want := int64(2); want <= 3; want++ {
		time.Sleep(20 * time.Millisecond)
		reclaimed, err := consumer.ReclaimStale(ctx, stream, testGroup, "consumer-b", 10*time.Millisecond, 10)
		require.NoError(t, err)
		require.Len(t, reclaimed, 1)
		assert.Equal(t, want, reclaimed[0].Deliveries)
	}
}

func TestConsumerGroup_DeadLetter_CopiesAndAcks(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)
	deadStream := stream + "-dead"

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))
	_, err := producer.Publish(ctx, stream, map[string]string{"task_id": "t-1"})
	require.NoError(t, err)
	entries, err := consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	require.NoError(t, consumer.DeadLetter(ctx, stream, testGroup, deadStream, entries[0], "max_deliveries_exceeded"))

	pending, err := client.XPending(ctx, stream, testGroup).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count, "a dead-lettered entry must leave the pending list")

	dead, err := client.XRange(ctx, deadStream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, dead, 1)
	assert.Equal(t, "t-1", dead[0].Values["task_id"])
	assert.Equal(t, "max_deliveries_exceeded", dead[0].Values["reason"])
	assert.Equal(t, entries[0].ID, dead[0].Values["source_entry_id"])
}

// A live consumer touches the entries it is still handling, so another
// consumer's reclaim leaves them alone and the delivery count does not move.
func TestConsumerGroup_Touch_KeepsEntryFromReclaim(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	stream := newStreamName(t)

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))
	_, err := producer.Publish(ctx, stream, map[string]string{"seq": "0"})
	require.NoError(t, err)
	entries, err := consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	const minIdle = 2 * time.Second
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, consumer.Touch(ctx, stream, testGroup, "consumer-a", []string{entries[0].ID}))
	time.Sleep(1500 * time.Millisecond)

	reclaimed, err := consumer.ReclaimStale(ctx, stream, testGroup, "consumer-b", minIdle, 10)
	require.NoError(t, err)
	assert.Empty(t, reclaimed, "a touched entry was reclaimed from a live consumer")

	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: testGroup, Start: "-", End: "+", Count: 10}).Result()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, int64(1), pending[0].RetryCount, "touching counted as a delivery")
}

type failXPending struct{}

func (failXPending) DialHook(next redis.DialHook) redis.DialHook { return next }
func (failXPending) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (failXPending) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xpending" {
			cmd.SetErr(errors.New("xpending unavailable"))
			return cmd.Err()
		}
		return next(ctx, cmd)
	}
}

// XAUTOCLAIM has already claimed and counted the entries by the time their
// counts are read, so failing to read a count must not drop them.
func TestConsumerGroup_ReclaimStale_KeepsClaimedEntriesWhenCountUnavailable(t *testing.T) {
	client := newTestClient(t).(*redis.Client)
	ctx := context.Background()
	stream := newStreamName(t)

	producer := valkeystream.NewProducer(client)
	consumer := valkeystream.NewConsumer(client)
	require.NoError(t, consumer.EnsureGroup(ctx, stream, testGroup))
	_, err := producer.Publish(ctx, stream, map[string]string{"seq": "0"})
	require.NoError(t, err)
	_, err = consumer.ReadGroup(ctx, stream, testGroup, "consumer-a", 1, time.Second)
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)

	client.AddHook(failXPending{})
	reclaimed, err := consumer.ReclaimStale(ctx, stream, testGroup, "consumer-b", 10*time.Millisecond, 10)

	require.NoError(t, err)
	require.Len(t, reclaimed, 1, "claimed entries were dropped")
	assert.Zero(t, reclaimed[0].Deliveries, "an unknown count must not be reported as a real one")
}
