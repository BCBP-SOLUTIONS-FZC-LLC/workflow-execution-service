package handler_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func claimRequest(key string, userID uuid.UUID) *http.Request {
	r := req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", map[string]any{"record_version": 1})
	r.Header.Set("Idempotency-Key", key)
	r.Header.Set("x-user-id", userID.String())
	return r
}

func countingClaimFake(calls *[]uuid.UUID) *fakeTaskService {
	return &fakeTaskService{
		claim: func(_ context.Context, _, _, userID uuid.UUID, _ int64) (*port.Task, error) {
			*calls = append(*calls, userID)
			return newTestTask(), nil
		},
	}
}

func TestIdempotency_APanickingHandlerReleasesTheClaimAndPanicsOn(t *testing.T) {
	cache := newFakeCacheStore()
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			panic("handler blew up")
		},
	}
	router := newRouter(newHandlerWithCache(fake, &fakeEligibilityChecker{}, cache))

	assert.PanicsWithValue(t, "handler blew up", func() { do(router, claimRequest("key-1", testUserID)) })

	assert.Empty(t, cache.data, "a panicked attempt must not leave the claim marker behind")
}

func TestIdempotency_ARetryRunsAgainWhenTheSuccessfulAnswerCouldNotBeStored(t *testing.T) {
	cache := &setFailingCacheStore{fakeCacheStore: newFakeCacheStore()}
	var calls []uuid.UUID
	router := newRouter(handler.New(handler.Services{Tasks: countingClaimFake(&calls), Eligibility: &fakeEligibilityChecker{}, Cache: cache, IdempotencyTTL: time.Hour}))

	first := do(router, claimRequest("key-1", testUserID))
	require.Equal(t, http.StatusAccepted, first.Code)
	assert.Empty(t, cache.data, "an answer that was not stored must not leave the claim marker behind")

	second := do(router, claimRequest("key-1", testUserID))

	assert.Equal(t, http.StatusAccepted, second.Code, second.Body.String())
	assert.Len(t, calls, 2, "the retry runs again instead of being refused as a replay")
}

func TestIdempotency_AnotherUserOfTheTenantDoesNotReceiveTheFirstUsersAnswer(t *testing.T) {
	cache := newFakeCacheStore()
	var calls []uuid.UUID
	router := newRouter(newHandlerWithCache(countingClaimFake(&calls), &fakeEligibilityChecker{}, cache))
	otherUser := uuid.MustParse("55555555-5555-5555-5555-555555555555")

	first := do(router, claimRequest("key-1", testUserID))
	second := do(router, claimRequest("key-1", otherUser))

	require.Equal(t, http.StatusAccepted, first.Code)
	require.Equal(t, http.StatusAccepted, second.Code, second.Body.String())
	assert.Equal(t, []uuid.UUID{testUserID, otherUser}, calls, "each user's request runs for that user")
	assert.Len(t, cache.data, 2, "each user holds their own stored answer")
}

func TestIdempotency_TheSameUserStillReceivesTheirStoredAnswer(t *testing.T) {
	cache := newFakeCacheStore()
	var calls []uuid.UUID
	router := newRouter(newHandlerWithCache(countingClaimFake(&calls), &fakeEligibilityChecker{}, cache))

	first := do(router, claimRequest("key-1", testUserID))
	second := do(router, claimRequest("key-1", testUserID))

	require.Equal(t, http.StatusAccepted, second.Code)
	assert.Equal(t, first.Body.String(), second.Body.String())
	assert.Equal(t, []uuid.UUID{testUserID}, calls)
}
