package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

type cursorReclaimStream struct {
	*dspfixStreamFake
	mu     sync.Mutex
	starts []string
	steps  []cursorStep
}

type cursorStep struct {
	next string
	err  error
}

func (c *cursorReclaimStream) ReclaimStale(_ context.Context, _, _, _, start string, _ time.Duration, _ int64) ([]valkeystream.Entry, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts = append(c.starts, start)
	if len(c.steps) == 0 {
		return nil, start, nil
	}
	step := c.steps[0]
	c.steps = c.steps[1:]
	return nil, step.next, step.err
}

func (c *cursorReclaimStream) startsSeen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.starts...)
}

func TestReclaimLoop_EachTickResumesWhereTheLastOneStoppedAndWrapsAtTheEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := &cursorReclaimStream{dspfixStreamFake: &dspfixStreamFake{}, steps: []cursorStep{
			{next: "1700-1"},
			{err: errors.New("valkey down")},
			{next: "0-0"},
			{next: "1800-4"},
		}}
		d := &deps{
			consumer: stream, dedup: newFakeCacheStore(), pools: nil,
			streamKey: "cursor-tasks", streamGroup: "cursor-group", consumerName: "cursor-worker",
			claimMinIdle: 30 * time.Second, batchSize: 2, maxDeliveries: 3,
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var wg sync.WaitGroup
		go func() {
			defer close(done)
			reclaimLoop(ctx, d, &wg, noopLogger{})
		}()

		for range 5 {
			time.Sleep(30 * time.Second)
			synctest.Wait()
		}
		cancel()
		<-done
		wg.Wait()

		assert.Equal(t, []string{"0", "1700-1", "1700-1", "0-0", "1800-4"}, stream.startsSeen(),
			"the first tick starts at 0, a failed tick keeps the cursor, and the end of the list starts over")
	})
}
