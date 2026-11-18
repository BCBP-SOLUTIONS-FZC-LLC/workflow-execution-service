package handler_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW14bLockUnavailable = errors.New("w14b lock store unavailable")

// w14bLockRecorder wraps the shared recency fake: it records each scope key
// passed to WithLock, whether the lock is held when the guard is consulted and
// committed, and can refuse the lock.
type w14bLockRecorder struct {
	*fakeRecencyGuard
	lockErr       error
	keys          []string
	held          bool
	shouldApplyIn bool
	commitIn      bool
}

func (r *w14bLockRecorder) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	r.keys = append(r.keys, scopeKey)
	if r.lockErr != nil {
		return r.lockErr
	}
	r.held = true
	defer func() { r.held = false }()
	return fn(ctx)
}

func (r *w14bLockRecorder) ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error) {
	r.shouldApplyIn = r.held
	return r.fakeRecencyGuard.ShouldApply(ctx, scopeKey, eventTime)
}

func (r *w14bLockRecorder) Commit(ctx context.Context, scopeKey string, eventTime time.Time) error {
	r.commitIn = r.held
	return r.fakeRecencyGuard.Commit(ctx, scopeKey, eventTime)
}

func TestHandleUserAvailabilityChanged_AppliesUnderTheUsersAvailabilityLock(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		userID uuid.UUID
	}{
		{name: "one user", userID: uuid.New()},
		{name: "another user gets another key", userID: uuid.New()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fakes := newEventsFakes()
			lock := &w14bLockRecorder{fakeRecencyGuard: newFakeRecencyGuard()}
			var gotIn port.UserAvailabilityInput
			var heldDuringApply bool
			fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error {
				gotIn, heldDuringApply = in, lock.held
				return nil
			}
			router := newInternalRouter(newEventsHandlerWithRecency(fakes, lock))

			w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
				"user_id": tc.userID.String(), "status": "ooo",
			}))

			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, []string{port.UserAvailabilityScope(testTenantID, tc.userID)}, lock.keys,
				"the key is the availability scope of the event's own tenant and user, the one a delegation reversal takes")
			assert.True(t, lock.shouldApplyIn, "the recency check runs under the lock")
			assert.True(t, heldDuringApply, "the availability change is applied under the lock")
			assert.True(t, lock.commitIn, "the recency mark is committed under the lock")
			assert.Equal(t, testTenantID, gotIn.TenantID)
			assert.Equal(t, tc.userID, gotIn.UserID)
		})
	}
}

func TestHandleUserAvailabilityChanged_LockFailureIsA500AndChangesNothing(t *testing.T) {
	t.Parallel()

	fakes := newEventsFakes()
	userID := uuid.New()
	lock := &w14bLockRecorder{fakeRecencyGuard: newFakeRecencyGuard(), lockErr: errW14bLockUnavailable}
	applied := 0
	fakes.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error {
		applied++
		return nil
	}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, lock))
	eventID := uuid.New()

	w := postEvent(router, envelope("UserAvailabilityChanged", eventID, testTenantID, time.Now(), map[string]any{
		"user_id": userID.String(), "status": "ooo",
	}))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var problem problemBody
	decodeJSON(t, w.Body, &problem)
	assert.Equal(t, "INTERNAL_ERROR", problem.Code)
	assert.Equal(t, []string{port.UserAvailabilityScope(testTenantID, userID)}, lock.keys, "the lock was asked for under the event's own key")
	assert.Zero(t, applied, "nothing is applied without the lock")
	assert.Empty(t, lock.last, "no recency mark is written")
	assert.Empty(t, fakes.processedEvents.seen, "the event is not marked processed, so the queue redelivers it")
}
