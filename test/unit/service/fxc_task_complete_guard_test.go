package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errFxcSignalDown = errors.New("temporal refused the signal")

// fxcStickyCache keeps a key when a delete fails, as a cache blip would.
type fxcStickyCache struct {
	*fakeCacheStore
	failDel bool
	dels    []string
}

func (c *fxcStickyCache) Del(ctx context.Context, keys ...string) error {
	c.dels = append(c.dels, keys...)
	if c.failDel {
		return errors.New("cache unreachable")
	}
	return c.fakeCacheStore.Del(ctx, keys...)
}

func fxcCompleteGuard(h *taHarness) (*fxcStickyCache, string) {
	cache := &fxcStickyCache{fakeCacheStore: newFakeCacheStore()}
	h.svc.Cache = cache
	return cache, "task-complete:" + h.task.ID.String()
}

func fxcComplete(h *taHarness) error {
	_, err := h.svc.Complete(context.Background(), h.tenantID, h.task.ID, h.callerID, json.RawMessage(`{}`), csdTaskVersion)
	return err
}

func TestTaskComplete_RetryWhileTheKeyIsHeldAndNoSignalWentOutAsksToRetryLater(t *testing.T) {
	h := newTaHarness(t, "single")
	cache, key := fxcCompleteGuard(h)
	cache.failDel = true
	h.temporalLog.err = errFxcSignalDown

	require.ErrorIs(t, fxcComplete(h), errFxcSignalDown)
	require.Len(t, h.temporalLog.signals, 1)
	assert.Equal(t, []string{key}, cache.dels, "the failed signal tried to free the key")
	assert.Equal(t, "reserved", cache.values[key], "the key is still held and nothing was delivered")

	err := fxcComplete(h)

	require.ErrorIs(t, err, port.ErrCallbackInFlight, "retry later, not a version conflict")
	assert.NotErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Len(t, h.temporalLog.signals, 1, "no second signal while the key is held")
	assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)

	delete(cache.values, key)
	h.temporalLog.err = nil

	require.NoError(t, fxcComplete(h), "after the key expires the retry is processed")
	assert.Len(t, h.temporalLog.signals, 2)
}

func TestTaskComplete_AnAttemptStillInFlightIsAnsweredRetryLater(t *testing.T) {
	h := newTaHarness(t, "single")
	cache, key := fxcCompleteGuard(h)
	cache.values[key] = "reserved"

	err := fxcComplete(h)

	require.ErrorIs(t, err, port.ErrCallbackInFlight)
	assert.Empty(t, h.temporalLog.signals)
}

func TestTaskComplete_ADeliveredSignalMakesARepeatAVersionConflict(t *testing.T) {
	h := newTaHarness(t, "single")
	cache, key := fxcCompleteGuard(h)

	require.NoError(t, fxcComplete(h))
	assert.Equal(t, "delivered", cache.values[key], "a sent signal is marked delivered")
	require.Len(t, h.temporalLog.signals, 1)

	err := fxcComplete(h)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Len(t, h.temporalLog.signals, 1, "the repeat sends nothing")
}

func TestTaskComplete_AFailedSignalFreesTheKeySoTheRetrySignalsAgain(t *testing.T) {
	h := newTaHarness(t, "single")
	cache, key := fxcCompleteGuard(h)
	h.temporalLog.err = errFxcSignalDown

	require.ErrorIs(t, fxcComplete(h), errFxcSignalDown)
	_, held := cache.values[key]
	assert.False(t, held)

	h.temporalLog.err = nil
	require.NoError(t, fxcComplete(h))
	assert.Len(t, h.temporalLog.signals, 2)
}

// fxcClockCache expires a key when the test clock passes its TTL.
type fxcClockCache struct {
	now     time.Duration
	values  map[string]string
	expires map[string]time.Duration
}

func newFxcClockCache() *fxcClockCache {
	return &fxcClockCache{values: map[string]string{}, expires: map[string]time.Duration{}}
}

func (c *fxcClockCache) live(key string) bool {
	if _, ok := c.values[key]; !ok {
		return false
	}
	if c.now >= c.expires[key] {
		delete(c.values, key)
		return false
	}
	return true
}

func (c *fxcClockCache) Get(_ context.Context, key string) (string, error) {
	if !c.live(key) {
		return "", nil
	}
	return c.values[key], nil
}

func (c *fxcClockCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.values[key], c.expires[key] = value, c.now+ttl
	return nil
}

func (c *fxcClockCache) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

func (c *fxcClockCache) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	if c.live(key) {
		return false, nil
	}
	c.values[key], c.expires[key] = value, c.now+ttl
	return true, nil
}

func (c *fxcClockCache) Ping(context.Context) error { return nil }

func TestTaskComplete_ADeliveredMarkOutlivesTheReservationWindow(t *testing.T) {
	h := newTaHarness(t, "single")
	cache := newFxcClockCache()
	h.svc.Cache = cache

	require.NoError(t, fxcComplete(h))
	require.Len(t, h.temporalLog.signals, 1)

	cache.now = 31 * time.Second // past the 30 s reservation window

	err := fxcComplete(h)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict, "the repeat is still answered from the delivered mark")
	assert.Len(t, h.temporalLog.signals, 1, "no second signal after the reservation window")

	cache.now = 6 * time.Minute // past the delivered mark too
	require.NoError(t, fxcComplete(h), "once the delivered mark expires the version check is the only guard")
}

func TestTaskComplete_ASignalThatMayHaveBeenAcceptedKeepsTheReservation(t *testing.T) {
	for name, signalErr := range map[string]error{
		"deadline exceeded": context.DeadlineExceeded,
		"cancelled":         context.Canceled,
		"wrapped deadline":  errors.Join(errors.New("update"), context.DeadlineExceeded),
	} {
		t.Run(name, func(t *testing.T) {
			h := newTaHarness(t, "single")
			cache, key := fxcCompleteGuard(h)
			h.temporalLog.err = signalErr

			require.ErrorIs(t, fxcComplete(h), signalErr)
			assert.Equal(t, "reserved", cache.values[key], "the signal may have reached the workflow, so the key is not released")
			assert.Empty(t, cache.dels)

			h.temporalLog.err = nil
			err := fxcComplete(h)

			require.ErrorIs(t, err, port.ErrCallbackInFlight, "a retry is asked to wait, not to signal again")
			assert.Len(t, h.temporalLog.signals, 1)
		})
	}
}
