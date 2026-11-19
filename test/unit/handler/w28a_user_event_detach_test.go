package handler_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w28aLockCtxRecorder records the context each lock and each apply ran under.
type w28aLockCtxRecorder struct {
	*fakeRecencyGuard
	keys     []string
	lockCtx  []w28aCtxState
	applyCtx []w28aCtxState
}

type w28aCtxState struct {
	err         error
	deadline    time.Time
	hasDeadline bool
}

func w28aStateOf(ctx context.Context) w28aCtxState {
	d, ok := ctx.Deadline()
	return w28aCtxState{err: ctx.Err(), deadline: d, hasDeadline: ok}
}

func (r *w28aLockCtxRecorder) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	r.keys = append(r.keys, scopeKey)
	r.lockCtx = append(r.lockCtx, w28aStateOf(ctx))
	return fn(ctx)
}

func TestHandleUserAvailabilityChanged_CallerGivesUp_LockAndApplyContinueUnderTheirOwnDeadline(t *testing.T) {
	t.Parallel()

	userID := uuid.New()
	fakes := newEventsFakes()
	lock := &w28aLockCtxRecorder{fakeRecencyGuard: newFakeRecencyGuard()}
	var gotIn port.UserAvailabilityInput
	fakes.oooAvailability.apply = func(ctx context.Context, in port.UserAvailabilityInput) error {
		gotIn = in
		lock.applyCtx = append(lock.applyCtx, w28aStateOf(ctx))
		return nil
	}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, lock))
	r := internalReq(http.MethodPost, eventRoute("UserAvailabilityChanged"), envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": userID.String(), "status": "ooo",
	}))
	gone, cancel := context.WithCancel(r.Context())
	cancel()

	w := do(router, r.WithContext(gone))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, []string{port.UserAvailabilityScope(testTenantID, userID)}, lock.keys)
	require.Len(t, lock.lockCtx, 1)
	require.Len(t, lock.applyCtx, 1)
	assert.Equal(t, testTenantID, gotIn.TenantID)
	assert.Equal(t, userID, gotIn.UserID)
	for name, st := range map[string]w28aCtxState{"waiting for the lock": lock.lockCtx[0], "applying the change": lock.applyCtx[0]} {
		assert.NoError(t, st.err, "a caller that gave up must not cancel %s", name)
		require.True(t, st.hasDeadline, "detached handling still needs its own bound while %s", name)
		assert.LessOrEqual(t, time.Until(st.deadline), 10*time.Minute, "the bound is at most 10 minutes while %s", name)
		assert.Greater(t, time.Until(st.deadline), 9*time.Minute, "the bound is about 10 minutes, not a short one, while %s", name)
	}
}
