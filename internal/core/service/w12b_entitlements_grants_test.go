package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW12bCommercialDown = errors.New("commercial is down")

// w12bGrantsReader answers every read from the same flags, fails while err is set, and records the tenant of each call.
type w12bGrantsReader struct {
	mu    sync.Mutex
	flags []string
	err   error
	calls []uuid.UUID
}

func (r *w12bGrantsReader) FeatureFlags(_ context.Context, tenantID uuid.UUID) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, tenantID)
	if r.err != nil {
		return nil, r.err
	}
	return r.flags, nil
}

func (r *w12bGrantsReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *w12bGrantsReader) callsSnapshot() []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uuid.UUID(nil), r.calls...)
}

type w12bClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *w12bClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *w12bClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func w12bStart() time.Time { return time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC) }

// w12bNoJitter is the injected jitter source that makes the factor exactly one.
func w12bNoJitter() float64 { return 0.5 }

func TestEntitlementsGrants_ASuccessfulReadAfterFailuresClearsTheBackoffAndTheNextCallInsideTheTTLReadsNothing(t *testing.T) {
	clock := &w12bClock{now: w12bStart()}
	reader := &w12bGrantsReader{flags: []string{"dedicated_workflow_queue"}, err: errW12bCommercialDown}
	grants, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: reader, TTL: time.Minute, FreshnessBound: 10 * time.Minute, Now: clock.Now, Rand: w12bNoJitter,
	})
	require.NoError(t, err)
	tenant := uuid.New()
	ctx := context.Background()

	_, err = grants.HasFeature(ctx, tenant, "dedicated_workflow_queue")
	require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	require.ErrorIs(t, err, errW12bCommercialDown)
	grants.mu.Lock()
	failed := *grants.entries[tenant]
	grants.mu.Unlock()
	require.ErrorIs(t, failed.lastErr, errW12bCommercialDown, "the failed read leaves its error behind")
	require.False(t, failed.retryAt.IsZero(), "and a backoff")
	require.False(t, failed.has)

	clock.advance(time.Second)
	reader.setErr(nil)
	_, err = grants.HasFeature(ctx, tenant, "dedicated_workflow_queue")
	require.ErrorIs(t, err, port.ErrEntitlementsUnavailable, "inside the backoff the read is not retried, even though the reader has recovered")
	require.Equal(t, []uuid.UUID{tenant}, reader.callsSnapshot())

	clock.advance(6 * time.Second)
	has, err := grants.HasFeature(ctx, tenant, "dedicated_workflow_queue")
	require.NoError(t, err)
	assert.True(t, has)
	other, err := grants.HasFeature(ctx, tenant, "a_flag_the_tenant_lacks")
	require.NoError(t, err)
	assert.False(t, other, "the read stored the whole set, not only the asked flag")
	assert.Equal(t, []uuid.UUID{tenant, tenant}, reader.callsSnapshot(), "the second read happened once the backoff had passed, and the other flag came from the cache")

	grants.mu.Lock()
	settled := *grants.entries[tenant]
	grants.mu.Unlock()
	assert.NoError(t, settled.lastErr, "the success clears the last error")
	assert.True(t, settled.retryAt.IsZero(), "and the backoff")
	assert.True(t, settled.has)
	assert.Equal(t, clock.Now(), settled.fetchedAt)

	clock.advance(10 * time.Second)
	has, err = grants.HasFeature(ctx, tenant, "dedicated_workflow_queue")
	require.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, []uuid.UUID{tenant, tenant}, reader.callsSnapshot(), "a call inside the TTL is a fresh hit")
}

func TestEntitlementsGrants_AFullCacheDropsAnEntryThatNeverHeldASetBeforeAnyHeldOne(t *testing.T) {
	now := w12bStart()
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: &w12bGrantsReader{}, TTL: time.Minute, FreshnessBound: 10 * time.Minute, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	neverHeld := uuid.New()
	g.entries[neverHeld] = &grantsEntry{retryAt: now.Add(5 * time.Second)}
	held := make([]uuid.UUID, entitlementsCacheCap-1)
	for i := range held {
		held[i] = uuid.New()
		g.entries[held[i]] = &grantsEntry{has: true, fetchedAt: now.Add(-time.Duration(i+1) * time.Millisecond)}
	}
	require.Len(t, g.entries, entitlementsCacheCap)

	g.mu.Lock()
	g.makeRoomLocked(now)
	g.mu.Unlock()

	assert.Len(t, g.entries, entitlementsCacheCap-1)
	assert.NotContains(t, g.entries, neverHeld, "its zero fetch time makes it the oldest")
	for _, id := range held {
		require.Contains(t, g.entries, id)
	}
}

func TestNewEntitlementsGrants_NilClockLogAndRandFallBackToWorkingDefaults(t *testing.T) {
	reader := &w12bGrantsReader{flags: []string{"dedicated_workflow_queue"}}
	grants, err := NewEntitlementsGrants(EntitlementsGrantsDeps{Reader: reader, TTL: time.Hour, FreshnessBound: 2 * time.Hour})
	require.NoError(t, err)

	assert.Equal(t, 32, cap(grants.slots), "the read limiter has 32 slots")
	assert.Empty(t, grants.slots, "and none is taken")
	require.NotNil(t, grants.entries)
	assert.Empty(t, grants.entries, "the cache starts empty")
	require.NotNil(t, grants.inflight)
	assert.Empty(t, grants.inflight)

	before := time.Now()
	ttls := map[time.Duration]struct{}{}
	for range 25 {
		tenant := uuid.New()
		has, err := grants.HasFeature(context.Background(), tenant, "dedicated_workflow_queue")
		require.NoError(t, err)
		assert.True(t, has)
		grants.mu.Lock()
		e := *grants.entries[tenant]
		grants.mu.Unlock()
		assert.False(t, e.fetchedAt.Before(before), "the default clock is the wall clock")
		assert.False(t, e.fetchedAt.After(time.Now()))
		assert.GreaterOrEqual(t, e.ttl, 48*time.Minute, "the default jitter keeps the TTL within 80 to 120 percent")
		assert.LessOrEqual(t, e.ttl, 72*time.Minute)
		ttls[e.ttl] = struct{}{}
	}
	assert.Greater(t, len(ttls), 1, "the default random source spreads the TTLs")

	reader.setErr(errW12bCommercialDown)
	assert.NotPanics(t, func() {
		_, err = grants.HasFeature(context.Background(), uuid.New(), "dedicated_workflow_queue")
	}, "a failed read logs through the default logger")
	require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	require.ErrorIs(t, err, errW12bCommercialDown)
}
