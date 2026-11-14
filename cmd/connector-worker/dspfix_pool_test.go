package main

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

const dspfixOtherType = "dspfix-other"

func dspfixLabelled(entry valkeystream.Entry, label string) valkeystream.Entry {
	entry.Fields["resolved_inputs"] = `{"label":"` + label + `"}`
	return entry
}

// dspfixHandOff runs dispatchBatch in the background and reports when it has handed every entry off.
func dspfixHandOff(ctx context.Context, w *dspfixWorker, wg *sync.WaitGroup, entries ...valkeystream.Entry) <-chan struct{} {
	returned := make(chan struct{})
	go func() {
		dispatchBatch(ctx, w.d, wg, entries, noopLogger{})
		close(returned)
	}()
	return returned
}

// A full pool for one connector type must not hold up entries of other types read in the same batch, and the
// runs of the full type never exceed its pool size.
func TestDispatchBatch_FullPoolDoesNotDelayOtherTypes(t *testing.T) {
	slow, fast := newDspfixGate(dspfixConnType, t), newDspfixGate(dspfixOtherType, t)
	fast.open()
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{
		slow.typ: dspfixPool(slow, 1), fast.typ: dspfixPool(fast, 1),
	})
	slow1, _, slow1Task := dspfixEntry("61-0", slow.typ, nil)
	slow2, _, slow2Task := dspfixEntry("62-0", slow.typ, nil)
	other, _, otherTask := dspfixEntry("63-0", fast.typ, nil)
	slow1, slow2, other = dspfixLabelled(slow1, "slow1"), dspfixLabelled(slow2, "slow2"), dspfixLabelled(other, "other")

	var wg sync.WaitGroup
	returned := dspfixHandOff(context.Background(), w, &wg, slow1, slow2, other)

	assert.Equal(t, "slow1", dspfixReceive(t, slow.started, "the first slow run"))
	assert.Equal(t, "other", dspfixReceive(t, fast.started, "the other type's run behind a full pool"))
	dspfixReceive(t, returned, "the batch to hand every entry off")
	assert.Equal(t, int32(1), slow.runs.Load(), "a second run of the full type started beyond its pool size")

	slow.open()
	assert.Equal(t, "slow2", dspfixReceive(t, slow.started, "the waiting run once a slot frees"))
	wg.Wait()

	assert.Equal(t, int32(1), slow.peak.Load(), "runs of one type overlapped beyond the pool size")
	assert.Equal(t, int32(2), slow.runs.Load())
	assert.ElementsMatch(t, []dspfixStreamCall{
		{stream: dspfixStream, group: dspfixGroup, id: "61-0"},
		{stream: dspfixStream, group: dspfixGroup, id: "62-0"},
		{stream: dspfixStream, group: dspfixGroup, id: "63-0"},
	}, w.stream.ackList())
	paths := map[string]bool{}
	for _, c := range w.callbacks.received() {
		paths[c.path] = true
	}
	assert.Equal(t, map[string]bool{
		dspfixCallbackPath(slow1Task, "complete"): true, dspfixCallbackPath(slow2Task, "complete"): true,
		dspfixCallbackPath(otherTask, "complete"): true,
	}, paths)
}

// An entry waiting for a pool slot is still pending: a shutdown frees it at once without a running job
// finishing; it is never run and not acknowledged, so the next worker picks it up.
func TestDispatchBatch_ShutdownWhileWaitingForAPoolSlot_ReturnsAtOnce(t *testing.T) {
	slow := newDspfixGate(dspfixConnType, t)
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{slow.typ: dspfixPool(slow, 1)})
	running, _, _ := dspfixEntry("64-0", slow.typ, nil)
	waiting, _, _ := dspfixEntry("65-0", slow.typ, nil)

	var runningWG sync.WaitGroup
	dspfixReceive(t, dspfixHandOff(context.Background(), w, &runningWG, running), "the running job to be handed off")
	dspfixReceive(t, slow.started, "the running job")

	ctx, shutdown := context.WithCancel(context.Background())
	var waitingWG sync.WaitGroup
	returned := dspfixHandOff(ctx, w, &waitingWG, waiting)
	shutdown()
	dspfixReceive(t, returned, "the waiting entry's hand-off to honour the shutdown")
	waitingWG.Wait()

	assert.Equal(t, int32(1), slow.runs.Load(), "the waiting entry ran")
	assert.Empty(t, w.stream.ackList(), "the waiting entry was acknowledged without running")
	assert.Equal(t, []string{"64-0"}, w.d.inflight.live(w.d.clock()), "the waiting entry is still tracked as in flight")

	slow.open()
	runningWG.Wait()
}

// At most one pool's worth of entries per type is parked behind a full pool. The entry after that makes the read
// loop wait for the type again, so memory stays bounded; entries of other types read before it were already
// dispatched, and a shutdown ends the wait at once.
func TestDispatchBatch_ParkedEntriesAreBoundedByThePoolSize(t *testing.T) {
	const poolSize = 2
	slow, fast, later := newDspfixGate(dspfixConnType, t), newDspfixGate(dspfixOtherType, t), newDspfixGate("dspfix-later", t)
	fast.open()
	later.open()
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{
		slow.typ: dspfixPool(slow, poolSize), fast.typ: dspfixPool(fast, 1), later.typ: dspfixPool(later, 1),
	})
	var batch []valkeystream.Entry
	for i, id := range []string{"81-0", "82-0", "83-0", "84-0"} {
		entry, _, _ := dspfixEntry(id, slow.typ, nil)
		batch = append(batch, dspfixLabelled(entry, "slow"+string(rune('1'+i))))
	}
	other, _, _ := dspfixEntry("85-0", fast.typ, nil)
	overflow, _, _ := dspfixEntry("86-0", slow.typ, nil)
	behind, _, _ := dspfixEntry("87-0", later.typ, nil)
	batch = append(batch, dspfixLabelled(other, "other"), dspfixLabelled(overflow, "overflow"), dspfixLabelled(behind, "behind"))

	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	var wg sync.WaitGroup
	returned := dspfixHandOff(ctx, w, &wg, batch...)

	dspfixReceive(t, slow.started, "the first run of the full type")
	dspfixReceive(t, slow.started, "the second run of the full type")
	assert.Equal(t, "other", dspfixReceive(t, fast.started, "the other type's run read before the overflow entry"))
	dspfixReceive(t, w.callbacks.seen, "the other type's report")

	select {
	case <-returned:
		t.Fatal("the read loop went past an entry of a type whose parked entries are at the bound")
	default:
	}
	assert.Empty(t, later.started, "an entry behind the overflow entry was dispatched while the loop should be waiting")
	assert.Equal(t, int32(poolSize), slow.runs.Load())
	assert.Subset(t, w.d.inflight.live(w.d.clock()), []string{"83-0", "84-0"}, "parked entries must stay in the heartbeat's in-flight set")

	shutdown()
	dspfixReceive(t, returned, "the wait for the full type to honour the shutdown")
	slow.open()
	wg.Wait()

	assert.Equal(t, int32(poolSize), slow.runs.Load(), "a parked or overflow entry ran after the shutdown")
	assert.Equal(t, int32(poolSize), slow.peak.Load())
	assert.Zero(t, later.runs.Load())
	assert.NotContains(t, w.d.inflight.live(w.d.clock()), "86-0", "the overflow entry is still tracked as in flight")
}
