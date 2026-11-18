package main

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDispatchLoop_AFailedReadWaitsInTheInjectedBackoffAfterExactlyOneRead(t *testing.T) {
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{})
	w.stream.script = []dspfixReadStep{{err: errW10bConnectionRefused}}
	entered := make(chan struct{})
	var once sync.Once
	w.d.readBackoff = func(ctx context.Context) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
	}
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		var wg sync.WaitGroup
		runDispatchLoop(ctx, w.d, &wg, noopLogger{})
		wg.Wait()
	}()

	dspfixReceive(t, entered, "the loop to enter the read backoff")

	assert.Len(t, w.stream.readList(), 1, "the loop must be held in the backoff after the one failed read")
	stop()
	select {
	case <-ended:
	case <-time.After(dspfixGuard):
		require.FailNow(t, "the loop did not end on shutdown")
	}
}
