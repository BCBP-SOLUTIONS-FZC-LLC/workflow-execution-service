package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

var errDspfixNoGroup = errors.New("valkeystream read group: NOGROUP No such key 'dspfix-stream' or consumer group 'dspfix-group' in XREADGROUP with GROUP option")

func runDspfixLoop(t *testing.T, w *dspfixWorker, ctx context.Context) {
	t.Helper()
	var wg sync.WaitGroup
	runDispatchLoop(ctx, w.d, &wg, noopLogger{})
	wg.Wait()
}

// A read that fails because the consumer group is gone (a Valkey flush) recreates the group and goes on reading
// and dispatching, with no restart.
func TestDispatchLoop_LostConsumerGroup_IsRecreatedAndDispatchResumes(t *testing.T) {
	conn := newDspfixGate(dspfixConnType, t)
	conn.open()
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{conn.typ: dspfixPool(conn, 1)})
	entry, tenantID, taskID := dspfixEntry("71-0", conn.typ, nil)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	w.stream.script = []dspfixReadStep{
		{err: errDspfixNoGroup},
		{entries: []valkeystream.Entry{entry}},
		{after: func() {
			select {
			case <-w.callbacks.seen:
			case <-time.After(dspfixGuard):
			}
			stop()
		}},
	}

	runDspfixLoop(t, w, ctx)

	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup}}, w.stream.ensureList(), "the group was not recreated once, on the configured stream")
	for _, r := range w.stream.readList() {
		assert.Equal(t, dspfixStreamCall{stream: dspfixStream, group: dspfixGroup}, r)
	}
	assert.GreaterOrEqual(t, len(w.stream.readList()), 3, "the loop did not resume reading after the group came back")
	assert.Equal(t, int32(1), conn.runs.Load())
	calls := w.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, dspfixCallbackPath(taskID, "complete"), calls[0].path)
	assert.Equal(t, tenantID.String(), calls[0].tenant)
	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup, id: "71-0"}}, w.stream.ackList())
}

// When recreating the group fails too, the loop keeps going and tries again on the next failed read; a shutdown
// during that wait ends it.
func TestDispatchLoop_GroupRecreateFailure_KeepsRetryingUntilShutdown(t *testing.T) {
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{})
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	w.stream.ensureErr = assert.AnError
	w.stream.onEnsure = stop
	w.stream.script = []dspfixReadStep{{err: errDspfixNoGroup}, {after: stop}}

	runDspfixLoop(t, w, ctx)

	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup}}, w.stream.ensureList())
	assert.Empty(t, w.stream.ackList())
}

// A read failure that is not a missing group does not touch the group.
func TestDispatchLoop_OtherReadFailure_DoesNotRecreateTheGroup(t *testing.T) {
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{})
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	w.stream.script = []dspfixReadStep{{err: errors.New("valkeystream read group: connection refused"), after: stop}}

	runDspfixLoop(t, w, ctx)

	assert.Empty(t, w.stream.ensureList())
}
