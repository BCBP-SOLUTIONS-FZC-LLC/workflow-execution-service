package service

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
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

const (
	testTTL   = time.Minute
	testBound = 10 * time.Minute
	testFlag  = "dedicated_workflow_queue"
)

// gatedReader is a reader the test steers: it can wait on a gate, fail, panic or answer with its context's error.
type gatedReader struct {
	mu        sync.Mutex
	calls     map[uuid.UUID]int
	flags     []string
	err       error
	gate      chan struct{}
	panicWith any
	ctxErr    bool
	active    int
	peak      int
}

func newGatedReader(flags ...string) *gatedReader {
	return &gatedReader{calls: map[uuid.UUID]int{}, flags: flags}
}

func (r *gatedReader) FeatureFlags(ctx context.Context, tenantID uuid.UUID) ([]string, error) {
	r.mu.Lock()
	r.calls[tenantID]++
	r.active++
	r.peak = max(r.peak, r.active)
	gate, boom, ctxErr := r.gate, r.panicWith, r.ctxErr
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	}()
	if boom != nil {
		panic(boom)
	}
	if gate != nil {
		<-gate
	}
	if ctxErr && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	return r.flags, nil
}

func (r *gatedReader) callsFor(id uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[id]
}

func (r *gatedReader) activeNow() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

func (r *gatedReader) peakActive() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peak
}

func (r *gatedReader) set(mutate func(*gatedReader)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mutate(r)
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// testRand is the injected jitter source; 0.5 means "no jitter" (a factor of exactly 1).
type testRand struct{ bits atomic.Uint64 }

func newTestRand(v float64) *testRand {
	r := &testRand{}
	r.set(v)
	return r
}

func (r *testRand) set(v float64) { r.bits.Store(math.Float64bits(v)) }
func (r *testRand) next() float64 { return math.Float64frombits(r.bits.Load()) }

type logLine struct {
	level  string
	msg    string
	fields map[string]any
}

// recLog records every line and runs onLog first, which the lock tests use to probe the cache's mutex.
type recLog struct {
	mu    sync.Mutex
	lines []logLine
	onLog func()
}

func (l *recLog) record(level, msg string, fields map[string]any) {
	if l.onLog != nil {
		l.onLog()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, logLine{level, msg, fields})
}

func (l *recLog) Debug(msg string, f map[string]any) { l.record("debug", msg, f) }
func (l *recLog) Info(msg string, f map[string]any)  { l.record("info", msg, f) }
func (l *recLog) Warn(msg string, f map[string]any)  { l.record("warn", msg, f) }
func (l *recLog) Error(msg string, f map[string]any) { l.record("error", msg, f) }

// waitFor returns the lines at level once there are n of them: a read logs after it unlocks, so it can trail settled.
func (l *recLog) waitFor(t *testing.T, level string, n int) []logLine {
	t.Helper()
	require.Eventually(t, func() bool { return len(l.at(level)) >= n }, 5*time.Second, time.Millisecond)
	return l.at(level)
}

func (l *recLog) at(level string) []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []logLine
	for _, line := range l.lines {
		if line.level == level {
			out = append(out, line)
		}
	}
	return out
}

type grantsHarness struct {
	g      *EntitlementsGrants
	reader *gatedReader
	clock  *testClock
	rnd    *testRand
	log    *recLog
}

func newHarness(t *testing.T, reader *gatedReader) *grantsHarness {
	t.Helper()
	h := &grantsHarness{
		reader: reader,
		clock:  &testClock{now: time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)},
		rnd:    newTestRand(0.5),
		log:    &recLog{},
	}
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: reader, TTL: testTTL, FreshnessBound: testBound, Now: h.clock.Now, Log: h.log, Rand: h.rnd.next,
	})
	require.NoError(t, err)
	h.g = g
	return h
}

func (h *grantsHarness) has(ctx context.Context, tenant uuid.UUID) (bool, error) {
	return h.g.HasFeature(ctx, tenant, testFlag)
}

func (h *grantsHarness) inflightCount() int {
	h.g.mu.Lock()
	defer h.g.mu.Unlock()
	return len(h.g.inflight)
}

func (h *grantsHarness) isInflight(id uuid.UUID) bool {
	h.g.mu.Lock()
	defer h.g.mu.Unlock()
	return h.g.inflight[id] != nil
}

func (h *grantsHarness) settled(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return h.inflightCount() == 0 }, 5*time.Second, time.Millisecond)
}

// prime reads the tenant once so a held set exists.
func (h *grantsHarness) prime(t *testing.T, tenant uuid.UUID) {
	t.Helper()
	has, err := h.has(context.Background(), tenant)
	require.NoError(t, err)
	require.True(t, has)
	h.settled(t)
}

func staleServed() float64 {
	return testutil.ToFloat64(observability.EntitlementsStaleServedTotal)
}

func TestNewEntitlementsGrants_RefusesWhatItCannotRunWith(t *testing.T) {
	reader := newGatedReader()
	tests := []struct {
		name string
		deps EntitlementsGrantsDeps
		want string
	}{
		{"a nil reader", EntitlementsGrantsDeps{TTL: testTTL, FreshnessBound: testBound}, "reader"},
		{"a zero TTL", EntitlementsGrantsDeps{Reader: reader, FreshnessBound: testBound}, "TTL"},
		{"a negative TTL", EntitlementsGrantsDeps{Reader: reader, TTL: -time.Second, FreshnessBound: testBound}, "TTL"},
		{"a bound below the TTL", EntitlementsGrantsDeps{Reader: reader, TTL: testTTL, FreshnessBound: testTTL - 1}, "freshness bound"},
		{"no bound", EntitlementsGrantsDeps{Reader: reader, TTL: testTTL}, "freshness bound"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := NewEntitlementsGrants(tt.deps)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.want)
			assert.Nil(t, g)
		})
	}
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{Reader: reader, TTL: testTTL, FreshnessBound: testTTL})
	require.NoError(t, err, "a bound equal to the TTL is allowed")
	assert.NotNil(t, g)
}

func TestEntitlementsGrants_TheTTLIsJitteredPerEntryBetweenEightyAndOneHundredTwentyPercent(t *testing.T) {
	obstest.Init(t)
	tests := []struct {
		name         string
		random       float64
		justFresh    time.Duration
		justStale    time.Duration
		wantTTLWhole time.Duration
	}{
		{"the lowest draw", 0, 47 * time.Second, 48 * time.Second, 48 * time.Second},
		{"no jitter", 0.5, 59 * time.Second, 60 * time.Second, 60 * time.Second},
		{"the highest draw", 0.999999, 71 * time.Second, 73 * time.Second, 72 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, newGatedReader(testFlag))
			h.rnd.set(tt.random)
			tenant := uuid.New()
			h.prime(t, tenant)
			h.g.mu.Lock()
			ttl := h.g.entries[tenant].ttl
			h.g.mu.Unlock()
			assert.InDelta(t, tt.wantTTLWhole.Seconds(), ttl.Seconds(), 0.001)

			before := staleServed()
			h.clock.advance(tt.justFresh)
			_, err := h.has(context.Background(), tenant)
			require.NoError(t, err)
			assert.Equal(t, before, staleServed(), "younger than the entry's TTL: served as is")
			assert.False(t, h.isInflight(tenant))

			h.clock.advance(tt.justStale - tt.justFresh)
			_, err = h.has(context.Background(), tenant)
			require.NoError(t, err)
			assert.Equal(t, before+1, staleServed(), "older than the entry's TTL: served stale")
		})
	}
}

func TestEntitlementsGrants_TheJitteredTTLNeverOutlivesTheFreshnessBound(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: reader, TTL: testTTL, FreshnessBound: testTTL, Now: h.clock.Now, Rand: newTestRand(0.999999).next,
	})
	require.NoError(t, err)
	tenant := uuid.New()
	_, err = g.HasFeature(context.Background(), tenant, testFlag)
	require.NoError(t, err)

	g.mu.Lock()
	ttl := g.entries[tenant].ttl
	g.mu.Unlock()

	assert.Equal(t, testTTL, ttl)
}

func TestEntitlementsGrants_TheBackoffIsJitteredToo(t *testing.T) {
	tests := []struct {
		name         string
		random       float64
		stillBlocked time.Duration
		reads        time.Duration
	}{
		{"the lowest draw", 0, 3900 * time.Millisecond, 4 * time.Second},
		{"the highest draw", 0.999999, 5900 * time.Millisecond, 6 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newGatedReader()
			reader.err = errors.New("down")
			h := newHarness(t, reader)
			h.rnd.set(tt.random)
			tenant := uuid.New()
			_, err := h.has(context.Background(), tenant)
			require.Error(t, err)

			h.clock.advance(tt.stillBlocked)
			_, err = h.has(context.Background(), tenant)
			require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
			assert.Equal(t, 1, reader.callsFor(tenant), "no read inside the backoff")

			h.clock.advance(tt.reads - tt.stillBlocked + time.Millisecond)
			_, err = h.has(context.Background(), tenant)
			require.Error(t, err)
			assert.Equal(t, 2, reader.callsFor(tenant), "the backoff is over")
		})
	}
}

func TestEntitlementsGrants_AStaleSetIsServedAtOnceWhileTheReadRunsAndTheNewValueAppearsAfterIt(t *testing.T) {
	obstest.Init(t)
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	gate := make(chan struct{})
	reader.set(func(r *gatedReader) { r.gate, r.flags = gate, []string{"sso_enabled"} })
	h.clock.advance(testTTL)
	before := staleServed()

	answered := make(chan bool, 1)
	go func() {
		has, err := h.has(context.Background(), tenant)
		assert.NoError(t, err)
		answered <- has
	}()
	select {
	case has := <-answered:
		assert.True(t, has, "the held set answers")
	case <-time.After(5 * time.Second):
		t.Fatal("the caller waited for the reader")
	}
	require.Eventually(t, func() bool { return reader.callsFor(tenant) == 2 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, 1, reader.activeNow(), "the reader is still running")
	assert.Equal(t, before+1, staleServed())

	close(gate)
	h.settled(t)
	has, err := h.has(context.Background(), tenant)
	require.NoError(t, err)
	assert.False(t, has, "the renewed set replaced the held one")
	assert.Equal(t, 2, reader.callsFor(tenant))
}

func TestEntitlementsGrants_AtOneAndAHalfTTLTheHeldSetIsServedWithoutWaitingForTheReader(t *testing.T) {
	obstest.Init(t)
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) })
	reader.set(func(r *gatedReader) { r.gate, r.flags = gate, []string{"sso_enabled"} })
	h.clock.advance(testTTL * 3 / 2)
	before := staleServed()

	has, err := h.has(context.Background(), tenant)

	require.NoError(t, err)
	assert.True(t, has, "the held value answered while the reader is still blocked")
	assert.Equal(t, before+1, staleServed())
	require.Eventually(t, func() bool { return reader.callsFor(tenant) == 2 }, 5*time.Second, time.Millisecond)
}

func TestEntitlementsGrants_AtTwoAndAHalfTTLTheCallerWaitsForTheReaderAndGetsTheNewValue(t *testing.T) {
	obstest.Init(t)
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	gate := make(chan struct{})
	reader.set(func(r *gatedReader) { r.gate, r.flags = gate, []string{"sso_enabled"} })
	h.clock.advance(testTTL * 5 / 2)
	before := staleServed()

	result := make(chan bool, 1)
	go func() {
		has, err := h.has(context.Background(), tenant)
		assert.NoError(t, err)
		result <- has
	}()
	require.Eventually(t, func() bool { return reader.callsFor(tenant) == 2 }, 5*time.Second, time.Millisecond)
	select {
	case <-result:
		t.Fatal("the caller must wait for the reader, not get the held value")
	case <-time.After(50 * time.Millisecond):
	}

	close(gate)
	assert.False(t, <-result, "the new value answered: the revoked flag is gone")
	assert.Equal(t, before, staleServed(), "a successful read is not a stale serve")
}

func TestEntitlementsGrants_AtTwoAndAHalfTTLAFailingReaderInsideTheBoundFallsBackToTheHeldSetAndCountsIt(t *testing.T) {
	obstest.Init(t)
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	reader.set(func(r *gatedReader) { r.err = errors.New("commercial is down") })
	h.clock.advance(testTTL * 5 / 2)
	before := staleServed()

	has, err := h.has(context.Background(), tenant)

	require.NoError(t, err)
	assert.True(t, has, "the held set is served when the read failed and it is within the bound")
	assert.Equal(t, 2, reader.callsFor(tenant), "the caller waited for a read first")
	assert.Equal(t, before+1, staleServed())
	assert.Len(t, h.log.waitFor(t, "warn", 1), 1)

	_, err = h.has(context.Background(), tenant)
	require.NoError(t, err, "inside the backoff the held set is served without a new read")
	assert.Equal(t, 2, reader.callsFor(tenant))
	assert.Equal(t, before+2, staleServed())
}

func TestEntitlementsGrants_TheWindowEndsAtTheBoundWhenTwiceTheTTLIsBeyondIt(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: reader, TTL: testTTL, FreshnessBound: testTTL * 3 / 2, Now: h.clock.Now, Rand: h.rnd.next,
	})
	require.NoError(t, err)
	tenant := uuid.New()
	_, err = g.HasFeature(context.Background(), tenant, testFlag)
	require.NoError(t, err)
	gate := make(chan struct{})
	reader.set(func(r *gatedReader) { r.gate, r.err = gate, errors.New("down") })
	h.clock.advance(testTTL * 3 / 2)

	result := make(chan error, 1)
	go func() {
		_, err := g.HasFeature(context.Background(), tenant, testFlag)
		result <- err
	}()
	require.Eventually(t, func() bool { return reader.callsFor(tenant) == 2 }, 5*time.Second, time.Millisecond)
	close(gate)

	require.ErrorIs(t, <-result, port.ErrEntitlementsUnavailable, "at the bound the held set is no longer served")
}

func TestEntitlementsGrants_ManyConcurrentCallersStartOneBackgroundRefreshPerTenant(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant, other := uuid.New(), uuid.New()
	h.prime(t, tenant)
	h.prime(t, other)
	gate := make(chan struct{})
	reader.set(func(r *gatedReader) { r.gate = gate })
	h.clock.advance(testTTL)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := tenant
			if i%2 == 1 {
				id = other
			}
			has, err := h.has(context.Background(), id)
			assert.NoError(t, err)
			assert.True(t, has)
		}()
	}
	wg.Wait()
	require.Eventually(t, func() bool { return reader.callsFor(tenant) == 2 && reader.callsFor(other) == 2 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, 2, h.inflightCount())

	close(gate)
	h.settled(t)
	assert.Equal(t, 2, reader.callsFor(tenant), "one refresh for the tenant")
	assert.Equal(t, 2, reader.callsFor(other))
}

func TestEntitlementsGrants_AFailedBackgroundRefreshKeepsServingTheHeldSetAndTheBackoffBlocksFurtherReads(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	reader.set(func(r *gatedReader) { r.err = errors.New("commercial is down") })
	h.clock.advance(testTTL * 3 / 2)

	has, err := h.has(context.Background(), tenant)
	require.NoError(t, err)
	assert.True(t, has)
	h.settled(t)
	require.Equal(t, 2, reader.callsFor(tenant))
	warns := h.log.waitFor(t, "warn", 1)
	require.Len(t, warns, 1, "one Warn per failed refresh")
	assert.Contains(t, warns[0].fields["error"], "commercial is down")

	h.clock.advance(3 * time.Second)
	for range 3 {
		has, err = h.has(context.Background(), tenant)
		require.NoError(t, err)
		assert.True(t, has)
		assert.False(t, h.isInflight(tenant), "inside the backoff no read starts")
	}
	assert.Equal(t, 2, reader.callsFor(tenant))

	h.clock.advance(2 * time.Second)
	_, err = h.has(context.Background(), tenant)
	require.NoError(t, err)
	assert.True(t, h.isInflight(tenant) || reader.callsFor(tenant) == 3, "after the backoff the next call starts a refresh")
	h.settled(t)
	assert.Equal(t, 3, reader.callsFor(tenant))
}

func TestEntitlementsGrants_PastTheBoundTheCallerWaitsForTheReadAndAFailureIsAnError(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	gate := make(chan struct{})
	reader.set(func(r *gatedReader) { r.gate, r.err = gate, errors.New("commercial is down") })
	h.clock.advance(testBound)

	result := make(chan error, 1)
	go func() {
		_, err := h.has(context.Background(), tenant)
		result <- err
	}()
	require.Eventually(t, func() bool { return reader.callsFor(tenant) == 2 }, 5*time.Second, time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("the caller must wait for the read, got %v", err)
	default:
	}

	close(gate)
	err := <-result
	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.ErrorContains(t, err, "commercial is down")
	assert.Len(t, h.log.waitFor(t, "error", 1), 1, "a read that leaves no usable set is logged at Error")
}

func TestEntitlementsGrants_PastTheBoundAWaitingCallerHonoursItsOwnContext(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) })
	reader.set(func(r *gatedReader) { r.gate = gate })
	h.clock.advance(testBound)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := h.has(ctx, tenant)
		result <- err
	}()
	require.Eventually(t, func() bool { return h.isInflight(tenant) }, 5*time.Second, time.Millisecond)

	cancel()

	err := <-result
	require.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
}

func TestEntitlementsGrants_ARunningReadIsNotCancelledWithTheCallerThatStartedIt(t *testing.T) {
	for name, stale := range map[string]bool{"a blocking read": false, "a background refresh": true} {
		t.Run(name, func(t *testing.T) {
			reader := newGatedReader(testFlag)
			h := newHarness(t, reader)
			tenant := uuid.New()
			if stale {
				h.prime(t, tenant)
				h.clock.advance(testTTL)
			}
			gate := make(chan struct{})
			reader.set(func(r *gatedReader) { r.gate, r.ctxErr = gate, true })
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				_, err := h.has(ctx, tenant)
				result <- err
			}()
			want := 1
			if stale {
				want = 2
			}
			require.Eventually(t, func() bool { return reader.callsFor(tenant) == want }, 5*time.Second, time.Millisecond)

			cancel()
			if !stale {
				require.ErrorIs(t, <-result, context.Canceled)
			} else {
				require.NoError(t, <-result)
			}
			close(gate)
			h.settled(t)

			has, err := h.has(context.Background(), tenant)
			require.NoError(t, err, "the read the cancelled caller started still produced the set")
			assert.True(t, has)
			assert.Equal(t, want, reader.callsFor(tenant))
			assert.Empty(t, h.log.at("warn"))
		})
	}
}

func TestEntitlementsGrants_NeverMoreThan32ReadsAreInFlight(t *testing.T) {
	reader := newGatedReader(testFlag)
	gate := make(chan struct{})
	reader.gate = gate
	h := newHarness(t, reader)

	const tenants = 100
	var wg sync.WaitGroup
	for range tenants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			has, err := h.has(context.Background(), uuid.New())
			assert.NoError(t, err)
			assert.True(t, has)
		}()
	}
	require.Eventually(t, func() bool { return reader.activeNow() == 32 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, 32, entitlementsMaxConcurrentReads)
	require.Eventually(t, func() bool { return h.inflightCount() == tenants }, 5*time.Second, time.Millisecond, "the rest wait for a slot")

	close(gate)
	wg.Wait()
	assert.Equal(t, 32, reader.peakActive())
	require.Eventually(t, func() bool { return len(h.g.slots) == 0 }, 5*time.Second, time.Millisecond, "every slot was given back")
}

func TestEntitlementsGrants_ABlockingReadWaitsForASlotWithItsContext(t *testing.T) {
	reader := newGatedReader(testFlag)
	gate := make(chan struct{})
	reader.gate = gate
	h := newHarness(t, reader)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.has(context.Background(), uuid.New())
		}()
	}
	require.Eventually(t, func() bool { return reader.activeNow() == 32 }, 5*time.Second, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := h.has(ctx, uuid.New())
		result <- err
	}()
	require.Eventually(t, func() bool { return h.inflightCount() == 33 }, 5*time.Second, time.Millisecond)

	cancel()

	require.ErrorIs(t, <-result, context.Canceled)
	close(gate)
	wg.Wait()
}

func TestEntitlementsGrants_ABackgroundRefreshWithoutAFreeSlotIsSkippedAndRetriedByALaterCall(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	held := uuid.New()
	h.prime(t, held)
	gate := make(chan struct{})
	reader.set(func(r *gatedReader) { r.gate = gate })
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.has(context.Background(), uuid.New())
		}()
	}
	require.Eventually(t, func() bool { return reader.activeNow() == 32 }, 5*time.Second, time.Millisecond)
	h.clock.advance(testTTL)

	has, err := h.has(context.Background(), held)

	require.NoError(t, err)
	assert.True(t, has, "the held set is served")
	assert.False(t, h.isInflight(held), "no slot, so no refresh was started")
	assert.Equal(t, 1, reader.callsFor(held))

	close(gate)
	wg.Wait()
	h.settled(t)
	_, err = h.has(context.Background(), held)
	require.NoError(t, err)
	h.settled(t)
	assert.Equal(t, 2, reader.callsFor(held), "a later call refreshed it")
}

func TestEntitlementsGrants_NothingIsLoggedWhileTheCacheLockIsHeld(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	var locked atomic.Bool
	h.log.onLog = func() {
		if h.g.mu.TryLock() {
			h.g.mu.Unlock()
			return
		}
		locked.Store(true)
	}
	tenant := uuid.New()
	h.prime(t, tenant)

	reader.set(func(r *gatedReader) { r.err = errors.New("down") })
	h.clock.advance(testTTL * 3 / 2)
	_, err := h.has(context.Background(), tenant)
	require.NoError(t, err)
	// no h.settled here: polling the cache lock would itself make a log call find it held
	require.Len(t, h.log.waitFor(t, "warn", 1), 1, "a failed refresh over a held set logs a Warn")

	_, err = h.has(context.Background(), uuid.New())
	require.Error(t, err)
	require.Len(t, h.log.waitFor(t, "error", 1), 1, "a failed cold read logs an Error")

	reader.set(func(r *gatedReader) { r.panicWith = "boom" })
	_, err = h.has(context.Background(), uuid.New())
	require.Error(t, err)
	require.Len(t, h.log.waitFor(t, "error", 3), 3, "a panic logs its own Error and the failed read's")

	assert.False(t, locked.Load(), "a log call found the cache lock held")
}

func TestEntitlementsGrants_ARecoveredPanicIsLoggedAtErrorWithItsStackCountedAndFailsTheRead(t *testing.T) {
	obstest.Init(t)
	reader := newGatedReader(testFlag)
	reader.panicWith = "reader blew up"
	h := newHarness(t, reader)
	tenant := uuid.New()
	before := testutil.ToFloat64(observability.EntitlementsReadPanicsTotal)

	_, err := h.has(context.Background(), tenant)

	require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.ErrorContains(t, err, "reader blew up")
	panics := h.log.waitFor(t, "error", 1)[0]
	assert.Equal(t, "entitlements reader panicked", panics.msg)
	assert.Equal(t, tenant.String(), panics.fields["tenant_id"])
	assert.Equal(t, "reader blew up", panics.fields["panic"])
	stack, _ := panics.fields["stack"].(string)
	assert.Contains(t, stack, "FeatureFlags", "the stack names the code that panicked")
	assert.Equal(t, before+1, testutil.ToFloat64(observability.EntitlementsReadPanicsTotal))

	h.clock.advance(entitlementsRetryBackoff * 2)
	reader.set(func(r *gatedReader) { r.panicWith = nil })
	has, err := h.has(context.Background(), tenant)
	require.NoError(t, err, "a panic is a failed read, not a poisoned tenant")
	assert.True(t, has)
}

func TestEntitlementsGrants_APanicInABackgroundRefreshKeepsTheHeldSetAndIsAWarningFailure(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenant := uuid.New()
	h.prime(t, tenant)
	reader.set(func(r *gatedReader) { r.panicWith = "boom" })
	h.clock.advance(testTTL * 3 / 2)

	has, err := h.has(context.Background(), tenant)
	require.NoError(t, err)
	assert.True(t, has)
	h.settled(t)

	assert.Len(t, h.log.waitFor(t, "warn", 1), 1)
	assert.Len(t, h.log.waitFor(t, "error", 1), 1, "the panic itself is at Error")
}

func TestEntitlementsGrants_TheAgeGaugeShowsTheOldestSetServedStaleInTheLastWindow(t *testing.T) {
	obstest.Init(t)
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	young, old := uuid.New(), uuid.New()
	h.prime(t, old)
	h.clock.advance(3 * time.Minute)
	h.prime(t, young)
	reader.set(func(r *gatedReader) { r.err = errors.New("down") })
	gauge := func() float64 { return testutil.ToFloat64(observability.EntitlementsOldestServedAgeSeconds) }

	h.clock.advance(2 * time.Minute)
	_, err := h.has(context.Background(), young)
	require.NoError(t, err)
	h.settled(t)
	assert.InDelta(t, 120, gauge(), 0.001, "the young set is 2 minutes old")

	_, err = h.has(context.Background(), old)
	require.NoError(t, err)
	assert.InDelta(t, 300, gauge(), 0.001, "the oldest wins")

	_, err = h.has(context.Background(), young)
	require.NoError(t, err)
	assert.InDelta(t, 300, gauge(), 0.001, "a younger serve does not lower it inside the window")

	reader.set(func(r *gatedReader) { r.err = nil })
	h.clock.advance(testTTL)
	_, err = h.has(context.Background(), young)
	require.NoError(t, err)
	h.settled(t)
	h.clock.advance(30 * time.Second)
	fresh := uuid.New()
	h.prime(t, fresh)
	h.clock.advance(31 * time.Second)
	_, err = h.has(context.Background(), fresh)
	require.NoError(t, err)
	assert.Zero(t, gauge(), "a window with no stale serve brings it back to 0")
}

func TestEntitlementsGrants_ManyTenantsAndCallersUnderFailuresAndClockJumpsStayConsistent(t *testing.T) {
	reader := newGatedReader(testFlag)
	h := newHarness(t, reader)
	tenants := make([]uuid.UUID, 60)
	for i := range tenants {
		tenants[i] = uuid.New()
	}
	var failing atomic.Bool
	h.rnd.set(0.3)
	var wg sync.WaitGroup
	for w := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for i := range 300 {
				switch {
				case w == 0 && i%10 == 0:
					h.clock.advance(time.Duration(rng.IntN(90)) * time.Second)
				case w == 1 && i%25 == 0:
					failing.Store(!failing.Load())
					reader.set(func(r *gatedReader) {
						r.err = nil
						if failing.Load() {
							r.err = errors.New("flaky")
						}
					})
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				has, err := h.has(ctx, tenants[rng.IntN(len(tenants))])
				cancel()
				if err != nil {
					assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
					continue
				}
				assert.True(t, has)
			}
		}()
	}
	wg.Wait()
	h.settled(t)

	require.Eventually(t, func() bool { return len(h.g.slots) == 0 }, 5*time.Second, time.Millisecond)
	assert.LessOrEqual(t, reader.peakActive(), 32)
}
