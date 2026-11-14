package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

type cwcliEvents struct {
	mu  sync.Mutex
	log []string
}

func (e *cwcliEvents) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.log = append(e.log, s)
}

func (e *cwcliEvents) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.log...)
}

// With a reclaim tick still running at shutdown, the dispatch loop returns only once that tick has finished.
func TestRunDispatchLoop_CancelWithAReclaimTickInFlight_ReturnsOnlyAfterTheReclaimFinished(t *testing.T) {
	events := &cwcliEvents{}
	reclaimEntered := make(chan struct{}, 1)
	readReturned := make(chan struct{})
	releaseReclaim := make(chan struct{})
	var finishedOnce sync.Once
	consumer := &cwcliConsumer{
		onRead: func(ctx context.Context) ([]valkeystream.Entry, error) {
			<-ctx.Done()
			events.add("read-returned")
			close(readReturned)
			return nil, ctx.Err()
		},
		onReclaim: func(context.Context) ([]valkeystream.Entry, error) {
			select {
			case reclaimEntered <- struct{}{}:
			default:
			}
			<-releaseReclaim
			finishedOnce.Do(func() { events.add("reclaim-finished") })
			return nil, nil
		},
	}
	d := &deps{
		consumer: consumer, claimMinIdle: time.Millisecond, streamKey: cwcliStream, streamGroup: cwcliGroup,
		consumerName: cwcliConsumerName, batchSize: 7, blockTimeout: 3 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		runDispatchLoop(ctx, d, &wg, &cwcliLogger{})
		events.add("loop-returned")
		close(done)
	}()

	<-reclaimEntered
	cancel()
	<-readReturned
	close(releaseReclaim)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the dispatch loop did not return")
	}

	assert.Equal(t, []string{"read-returned", "reclaim-finished", "loop-returned"}, events.all())
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	require.Len(t, consumer.readArgs, 1)
	assert.Equal(t, cwcliReadArgs{stream: cwcliStream, group: cwcliGroup, consumer: cwcliConsumerName, count: 7, block: 3 * time.Second},
		consumer.readArgs[0], "the read uses the configured stream, group, consumer name, batch size and block time")
}

func TestRunDispatchLoop_ContextCancelledBeforeStart_ReturnsWithoutReading(t *testing.T) {
	consumer := &cwcliConsumer{}
	d := &deps{consumer: consumer, claimMinIdle: time.Hour, streamKey: cwcliStream, streamGroup: cwcliGroup, consumerName: cwcliConsumerName}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wg sync.WaitGroup

	runDispatchLoop(ctx, d, &wg, &cwcliLogger{})

	require.Zero(t, consumer.readCount(), "no ReadGroup call after cancellation")
	assert.Zero(t, consumer.reclaimCount())
}
