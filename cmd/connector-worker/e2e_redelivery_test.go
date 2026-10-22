//go:build e2e

// Redelivery behaviour of the real read, reclaim and heartbeat loops against
// a real Valkey: the unit tests fake the stream, and delivery counts, idle
// times and XCLAIM semantics are exactly what a fake gets wrong.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const (
	redeliveryStream = "connector-tasks-redelivery"
	redeliveryGroup  = "connector-worker"
	redeliveryDead   = "connector-tasks-redelivery-dead"
	redeliveryIdle   = 3 * time.Second
)

type reclaimCounter struct {
	streamConsumer
	reclaimed atomic.Int32
}

func (c *reclaimCounter) ReclaimStale(ctx context.Context, streamKey, group, consumerName string, minIdle time.Duration, count int64) ([]valkeystream.Entry, error) {
	entries, err := c.streamConsumer.ReclaimStale(ctx, streamKey, group, consumerName, minIdle, count)
	c.reclaimed.Add(int32(len(entries)))
	return entries, err
}

type sleepyConnector struct {
	countingConnector
	sleep time.Duration
}

func (c *sleepyConnector) Execute(ctx context.Context, in map[string]any) (map[string]any, error) {
	time.Sleep(c.sleep)
	return c.countingConnector.Execute(ctx, in)
}

type redeliveryWorker struct {
	d       *deps
	stream  *reclaimCounter
	stopped chan struct{}
}

func startRedeliveryWorker(t *testing.T, client *redis.Client, name string, conn interface {
	Type() string
	Execute(context.Context, map[string]any) (map[string]any, error)
}, timeout time.Duration, maxDeliveries int64, callbackURL string) *redeliveryWorker {
	t.Helper()
	stream := &reclaimCounter{streamConsumer: valkeystream.NewConsumer(client)}
	d := &deps{
		valkeyClient: client,
		consumer:     stream,
		dedup:        valkey.NewCache(client),
		pools: map[string]*typePool{conn.Type(): {
			connectorType: conn.Type(), sem: make(chan struct{}, 2), timeout: timeout,
			connector: conn,
		}},
		completion:    newCompletionClient(callbackURL, "token", 2*time.Second),
		streamKey:     redeliveryStream,
		streamGroup:   redeliveryGroup,
		consumerName:  name,
		blockTimeout:  200 * time.Millisecond,
		claimMinIdle:  redeliveryIdle,
		batchSize:     10,
		maxDeliveries: maxDeliveries,
		deadLetterKey: redeliveryDead,
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &redeliveryWorker{d: d, stream: stream, stopped: make(chan struct{})}
	var wg sync.WaitGroup
	go heartbeatLoop(ctx, d, noopLogger{})
	go func() {
		runDispatchLoop(ctx, d, &wg, noopLogger{})
		wg.Wait()
		close(w.stopped)
	}()
	t.Cleanup(func() {
		cancel()
		<-w.stopped
	})
	return w
}

func newRedeliveryStream(t *testing.T) *redis.Client {
	t.Helper()
	client := newValkeyClient(t, fixtures.NewTestValkey(t))
	require.NoError(t, valkeystream.NewConsumer(client).EnsureGroup(context.Background(), redeliveryStream, redeliveryGroup))
	return client
}

func publishConnectorEntry(t *testing.T, client *redis.Client, tenantID uuid.UUID, connectorType string) {
	t.Helper()
	_, err := valkeystream.NewProducer(client).Publish(context.Background(), redeliveryStream, map[string]string{
		"tenant_id": tenantID.String(), "task_id": uuid.NewString(),
		"department_id": uuid.NewString(), "connector_type": connectorType,
	})
	require.NoError(t, err)
}

func pendingCount(t *testing.T, client *redis.Client) int64 {
	t.Helper()
	pending, err := client.XPending(context.Background(), redeliveryStream, redeliveryGroup).Result()
	require.NoError(t, err)
	return pending.Count
}

// The original §2a defect: a failed callback left the entry pending, and
// every reclaim ran the connector again.
func TestE2E_Redelivery_FailedCallbacksDoNotRerunTheConnector(t *testing.T) {
	client := newRedeliveryStream(t)
	var callbacks atomic.Int32
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if callbacks.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(callbackSrv.Close)
	tenantID := uuid.New()
	conn := &countingConnector{typ: "counting"}
	startRedeliveryWorker(t, client, "worker-a", conn, time.Second, 10, callbackSrv.URL)

	publishConnectorEntry(t, client, tenantID, conn.typ)

	require.Eventually(t, func() bool { return callbacks.Load() >= 3 && pendingCount(t, client) == 0 }, 40*time.Second, 200*time.Millisecond,
		"the entry was never acked after its callback recovered")
	assert.Equal(t, int32(1), conn.calls.Load(), "the connector ran again on redelivery")
	dead, err := client.XLen(context.Background(), redeliveryDead).Result()
	require.NoError(t, err)
	assert.Zero(t, dead)
}

// A run longer than the reclaim wait, with a second worker reclaiming: the
// heartbeat keeps the entry with its live consumer.
func TestE2E_Redelivery_SlowRunIsNotReclaimedFromALiveWorker(t *testing.T) {
	client := newRedeliveryStream(t)
	callbackURL := func() string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
		t.Cleanup(srv.Close)
		return srv.URL
	}()
	tenantID := uuid.New()
	conn := &sleepyConnector{countingConnector: countingConnector{typ: "sleepy"}, sleep: 2*redeliveryIdle + time.Second}
	a := startRedeliveryWorker(t, client, "worker-a", conn, 20*time.Second, 10, callbackURL)
	b := startRedeliveryWorker(t, client, "worker-b", conn, 20*time.Second, 10, callbackURL)

	publishConnectorEntry(t, client, tenantID, conn.typ)

	require.Eventually(t, func() bool { return conn.calls.Load() == 1 && pendingCount(t, client) == 0 }, 30*time.Second, 200*time.Millisecond,
		"the slow run never reported")
	assert.Zero(t, a.stream.reclaimed.Load()+b.stream.reclaimed.Load(), "an entry was reclaimed from a live worker")
}

// An outcome the callback keeps refusing can never be reported: the entry is
// dead-lettered once past its delivery cap, and the connector ran only once.
func TestE2E_Redelivery_UnreportableEntryIsDeadLettered(t *testing.T) {
	client := newRedeliveryStream(t)
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusConflict) }))
	t.Cleanup(callbackSrv.Close)
	conn := &countingConnector{typ: "counting"}
	startRedeliveryWorker(t, client, "worker-a", conn, time.Second, 2, callbackSrv.URL)

	publishConnectorEntry(t, client, uuid.New(), conn.typ)

	require.Eventually(t, func() bool {
		n, err := client.XLen(context.Background(), redeliveryDead).Result()
		return err == nil && n == 1
	}, 40*time.Second, 200*time.Millisecond, "the entry was never dead-lettered")
	assert.Zero(t, pendingCount(t, client))
	assert.Equal(t, int32(1), conn.calls.Load(), "a refused callback re-ran the connector")
	dead, err := client.XRange(context.Background(), redeliveryDead, "-", "+").Result()
	require.NoError(t, err)
	assert.Equal(t, "max_deliveries_exceeded", dead[0].Values["reason"])
}
