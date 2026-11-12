package service_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type fakeEntitlementsReader struct {
	mu    sync.Mutex
	calls map[uuid.UUID]int
	flags map[uuid.UUID][]string
	err   error
	gate  chan struct{}
	panic bool
}

func newFakeEntitlementsReader() *fakeEntitlementsReader {
	return &fakeEntitlementsReader{calls: map[uuid.UUID]int{}, flags: map[uuid.UUID][]string{}}
}

func (r *fakeEntitlementsReader) FeatureFlags(ctx context.Context, tenantID uuid.UUID) ([]string, error) {
	r.mu.Lock()
	r.calls[tenantID]++
	err, flags, gate, boom := r.err, r.flags[tenantID], r.gate, r.panic
	r.mu.Unlock()
	if boom {
		panic("reader blew up")
	}
	if gate != nil {
		<-gate
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return flags, nil
}

func (r *fakeEntitlementsReader) callCount(tenantID uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[tenantID]
}

func (r *fakeEntitlementsReader) set(tenantID uuid.UUID, err error, flags ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
	r.flags[tenantID] = flags
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

const grantFlag = "dedicated_workflow_queue"

// newGrants fixes the jitter draw at 0.5, which is a factor of exactly 1, so the TTL is 60 s and the backoff 5 s.
func newGrants(t *testing.T, r port.EntitlementsReader) (*service.EntitlementsGrants, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)}
	grants, err := service.NewEntitlementsGrants(service.EntitlementsGrantsDeps{
		Reader: r, TTL: 60 * time.Second, FreshnessBound: 10 * time.Minute, Now: clock.Now,
		Rand: func() float64 { return 0.5 },
	})
	require.NoError(t, err)
	return grants, clock
}

func TestEntitlementsGrants_ReadsOnceAndServesTheCacheInsideTheTTL(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, "sso_enabled", grantFlag)
	grants, clock := newGrants(t, reader)

	for range 3 {
		has, err := grants.HasFeature(context.Background(), tenant, grantFlag)
		require.NoError(t, err)
		assert.True(t, has)
		clock.advance(10 * time.Second)
	}
	has, err := grants.HasFeature(context.Background(), tenant, "sso_enabled")
	require.NoError(t, err)
	assert.True(t, has, "the whole flag set is cached, not one flag")
	has, err = grants.HasFeature(context.Background(), tenant, "unknown_flag")
	require.NoError(t, err)
	assert.False(t, has)

	assert.Equal(t, 1, reader.callCount(tenant))
}

func TestEntitlementsGrants_ServesTheHeldSetOnceTheTTLHasPassedAndSeesTheRevocationAfterTheRefresh(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, grantFlag)
	grants, clock := newGrants(t, reader)
	has, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)
	require.True(t, has)

	reader.set(tenant, nil, "sso_enabled")
	clock.advance(60 * time.Second)

	has, err = grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)
	assert.True(t, has, "the held set answers while the refresh runs")
	require.Eventually(t, func() bool {
		has, err := grants.HasFeature(context.Background(), tenant, grantFlag)
		return err == nil && !has
	}, 5*time.Second, time.Millisecond, "a revoked grant is seen once the refresh has landed")
	assert.Equal(t, 2, reader.callCount(tenant))
}

func TestEntitlementsGrants_ServesTheHeldSetWhenARefreshFailsInsideTheBound(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, grantFlag)
	grants, clock := newGrants(t, reader)
	_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)

	reader.set(tenant, errors.New("commercial is down"))
	clock.advance(2 * time.Minute)

	has, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)
	assert.True(t, has)
}

func TestEntitlementsGrants_ErrorsOnceTheHeldSetIsOlderThanTheBound(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, grantFlag)
	grants, clock := newGrants(t, reader)
	_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)

	reader.set(tenant, errors.New("commercial is down"))
	clock.advance(10 * time.Minute)

	_, err = grants.HasFeature(context.Background(), tenant, grantFlag)
	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.ErrorContains(t, err, "commercial is down")
}

func TestEntitlementsGrants_ErrorsWhenTheTenantWasNeverRead(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, errors.New("commercial is down"))
	grants, _ := newGrants(t, reader)

	has, err := grants.HasFeature(context.Background(), tenant, grantFlag)

	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.False(t, has, "a failed read is never turned into an answer")
}

func TestEntitlementsGrants_AFailedReadIsNeverCachedAsAValue(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, errors.New("boom"))
	grants, clock := newGrants(t, reader)
	_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.Error(t, err)

	reader.set(tenant, nil, grantFlag)
	clock.advance(6 * time.Second)

	has, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)
	assert.True(t, has)
}

func TestEntitlementsGrants_BacksOffForFiveSecondsAfterAFailedRead(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, errors.New("boom"))
	grants, clock := newGrants(t, reader)
	_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.Error(t, err)
	require.Equal(t, 1, reader.callCount(tenant))

	clock.advance(4 * time.Second)
	_, err = grants.HasFeature(context.Background(), tenant, grantFlag)
	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.Equal(t, 1, reader.callCount(tenant), "no reader call inside the backoff window")

	clock.advance(time.Second)
	_, err = grants.HasFeature(context.Background(), tenant, grantFlag)
	require.Error(t, err)
	assert.Equal(t, 2, reader.callCount(tenant), "the window is over")
}

func TestEntitlementsGrants_TheBackoffServesTheHeldSetToo(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, grantFlag)
	grants, clock := newGrants(t, reader)
	_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)
	reader.set(tenant, errors.New("boom"))
	clock.advance(time.Minute)
	_, err = grants.HasFeature(context.Background(), tenant, grantFlag)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return reader.callCount(tenant) == 2 }, 5*time.Second, time.Millisecond)

	clock.advance(2 * time.Second)
	has, err := grants.HasFeature(context.Background(), tenant, grantFlag)

	require.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, 2, reader.callCount(tenant))
}

func TestEntitlementsGrants_ConcurrentCallersShareOneRead(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, grantFlag)
	reader.gate = make(chan struct{})
	grants, _ := newGrants(t, reader)

	const callers = 20
	var started, done sync.WaitGroup
	results := make([]bool, callers)
	errs := make([]error, callers)
	started.Add(callers)
	done.Add(callers)
	for i := range callers {
		go func() {
			defer done.Done()
			started.Done()
			results[i], errs[i] = grants.HasFeature(context.Background(), tenant, grantFlag)
		}()
	}
	started.Wait()
	require.Eventually(t, func() bool { return reader.callCount(tenant) == 1 }, time.Second, time.Millisecond)
	close(reader.gate)
	done.Wait()

	for i := range callers {
		require.NoError(t, errs[i])
		assert.True(t, results[i])
	}
	assert.Equal(t, 1, reader.callCount(tenant))
}

func TestEntitlementsGrants_ACallerThatGivesUpLeavesTheReadRunningForTheOthers(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.set(tenant, nil, grantFlag)
	reader.gate = make(chan struct{})
	grants, _ := newGrants(t, reader)

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := grants.HasFeature(ctx, tenant, grantFlag)
		first <- err
	}()
	require.Eventually(t, func() bool { return reader.callCount(tenant) == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-first, context.Canceled)

	var second atomic.Bool
	secondDone := make(chan error, 1)
	go func() {
		has, err := grants.HasFeature(context.Background(), tenant, grantFlag)
		second.Store(has)
		secondDone <- err
	}()
	close(reader.gate)

	require.NoError(t, <-secondDone)
	assert.True(t, second.Load())
	assert.Equal(t, 1, reader.callCount(tenant), "the cancelled caller's read served the next caller")
}

func TestEntitlementsGrants_APanickingReaderIsAnUnavailableError(t *testing.T) {
	reader, tenant := newFakeEntitlementsReader(), uuid.New()
	reader.panic = true
	grants, _ := newGrants(t, reader)

	_, err := grants.HasFeature(context.Background(), tenant, grantFlag)

	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
}

func TestEntitlementsGrants_TenantsDoNotShareAnswersOrBackoff(t *testing.T) {
	reader, granted, plain, broken := newFakeEntitlementsReader(), uuid.New(), uuid.New(), uuid.New()
	reader.set(granted, nil, grantFlag)
	reader.set(plain, nil, "sso_enabled")
	grants, _ := newGrants(t, reader)

	has, err := grants.HasFeature(context.Background(), granted, grantFlag)
	require.NoError(t, err)
	assert.True(t, has)
	has, err = grants.HasFeature(context.Background(), plain, grantFlag)
	require.NoError(t, err)
	assert.False(t, has)

	reader.set(broken, errors.New("boom"))
	_, err = grants.HasFeature(context.Background(), broken, grantFlag)
	require.Error(t, err)
	has, err = grants.HasFeature(context.Background(), granted, grantFlag)
	require.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, 1, reader.callCount(granted))
}
