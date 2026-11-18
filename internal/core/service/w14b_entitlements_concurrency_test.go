package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const w14bWait = 5 * time.Second

func TestEntitlementsGrants_TheThirtyThirdReadWhoseCallerGivesUpIsUnavailableYetStillFillsTheCache(t *testing.T) {
	reader := newGatedReader(testFlag)
	gate := make(chan struct{})
	reader.gate = gate
	var openGate sync.Once
	release := func() { openGate.Do(func() { close(gate) }) }
	t.Cleanup(release)
	h := newHarness(t, reader)

	var wg sync.WaitGroup
	for range entitlementsMaxConcurrentReads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.has(context.Background(), uuid.New())
		}()
	}
	require.Eventually(t, func() bool { return reader.activeNow() == entitlementsMaxConcurrentReads }, w14bWait, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := uuid.New()
	result := make(chan error, 1)
	go func() {
		_, err := h.has(ctx, waiting)
		result <- err
	}()
	require.Eventually(t, func() bool { return h.isInflight(waiting) }, w14bWait, time.Millisecond)
	assert.Zero(t, reader.callsFor(waiting), "no slot is free, so the reader has not been called for it")

	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(w14bWait):
		t.Fatal("the caller whose context ended was not released")
	}

	release()
	wg.Wait()
	require.Eventually(t, func() bool { return reader.callsFor(waiting) == 1 && h.inflightCount() == 0 }, w14bWait, time.Millisecond,
		"the read the caller gave up on still runs once a slot frees")
	has, err := h.has(context.Background(), waiting)
	require.NoError(t, err)
	assert.True(t, has, "the set it read is in the cache")
	assert.Equal(t, 1, reader.callsFor(waiting), "the later call was served from the cache, not a new read")
	require.Eventually(t, func() bool { return len(h.g.slots) == 0 }, w14bWait, time.Millisecond, "every slot was given back")
}

// w14bPanicAfterGateReader blocks every read on the gate, then panics for the
// first panicsLeft reads and answers with the flag after that.
type w14bPanicAfterGateReader struct {
	gate       chan struct{}
	calls      atomic.Int32
	panicsLeft atomic.Int32
}

func (r *w14bPanicAfterGateReader) FeatureFlags(_ context.Context, _ uuid.UUID) ([]string, error) {
	r.calls.Add(1)
	<-r.gate
	if r.panicsLeft.Add(-1) >= 0 {
		panic("reader blew up")
	}
	return []string{testFlag}, nil
}

func TestEntitlementsGrants_APanickingReadFailsEveryCallerWithThePanicFromOneRead(t *testing.T) {
	obstest.Init(t)
	reader := &w14bPanicAfterGateReader{gate: make(chan struct{})}
	reader.panicsLeft.Store(1)
	var openGate sync.Once
	release := func() { openGate.Do(func() { close(reader.gate) }) }
	t.Cleanup(release)
	clock := &testClock{now: time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)}
	rnd, log := newTestRand(0.5), &recLog{}
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: reader, TTL: testTTL, FreshnessBound: testBound, Now: clock.Now, Log: log, Rand: rnd.next,
	})
	require.NoError(t, err)
	h := &grantsHarness{g: g, clock: clock, rnd: rnd, log: log}
	tenant := uuid.New()
	before := testutil.ToFloat64(observability.EntitlementsReadPanicsTotal)

	const callers = 5
	errs := make(chan error, callers)
	var started sync.WaitGroup
	started.Add(callers)
	for range callers {
		go func() {
			started.Done()
			_, err := h.has(context.Background(), tenant)
			errs <- err
		}()
	}
	started.Wait()
	require.Eventually(t, func() bool { return reader.calls.Load() == 1 }, w14bWait, time.Millisecond)
	release()

	for range callers {
		select {
		case err := <-errs:
			require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
			assert.ErrorContains(t, err, "reading entitlements panicked: reader blew up")
		case <-time.After(w14bWait):
			t.Fatal("a waiting caller was not released after the panic")
		}
	}
	h.settled(t)
	require.Eventually(t, func() bool { return len(g.slots) == 0 }, w14bWait, time.Millisecond, "the slot is given back after a panic")
	assert.Equal(t, int32(1), reader.calls.Load(), "the source was read exactly once across all the callers")
	assert.Equal(t, before+1, testutil.ToFloat64(observability.EntitlementsReadPanicsTotal), "one panic counted, not one per caller")

	var panicLines []logLine
	for _, line := range h.log.waitFor(t, "error", 1) {
		if line.msg == "entitlements reader panicked" {
			panicLines = append(panicLines, line)
		}
	}
	require.Len(t, panicLines, 1)
	assert.Equal(t, tenant.String(), panicLines[0].fields["tenant_id"])
	assert.Equal(t, "reader blew up", panicLines[0].fields["panic"])
	assert.NotEmpty(t, panicLines[0].fields["stack"])

	_, err = h.has(context.Background(), tenant)
	require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.Equal(t, int32(1), reader.calls.Load(), "inside the backoff no new read is made")

	h.clock.advance(entitlementsRetryBackoff + time.Second)
	has, err := h.has(context.Background(), tenant)
	require.NoError(t, err, "once the backoff has passed the tenant is read again")
	assert.True(t, has)
	assert.Equal(t, int32(2), reader.calls.Load())
}
