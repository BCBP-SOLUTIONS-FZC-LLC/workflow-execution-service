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

func TestIdempotency_TheClaimMarkerLivesBrieflyAndOnlyTheStoredAnswerKeepsTheConfiguredLifetime(t *testing.T) {
	const fiveMinutes = 5 * time.Minute
	cases := []struct {
		name      string
		ttl       time.Duration
		wantClaim func(t *testing.T, claim time.Duration)
	}{
		{"a day-long lifetime", 24 * time.Hour, func(t *testing.T, claim time.Duration) {
			assert.Positive(t, claim)
			assert.LessOrEqual(t, claim, fiveMinutes, "a pod that dies mid-request must not hold the key for the answer lifetime")
		}},
		{"a configured lifetime shorter than the claim limit", 10 * time.Second, func(t *testing.T, claim time.Duration) {
			assert.Equal(t, 10*time.Second, claim, "the claim never outlives the answer lifetime")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := w25aNewCache()
			tasks := &fakeTaskService{
				claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
					return newTestTask(), nil
				},
			}
			router := newRouter(handler.New(handler.Services{Tasks: tasks, Eligibility: &fakeEligibilityChecker{}, Cache: cache, IdempotencyTTL: tc.ttl}))

			w := w25aServe(router, w25aKeyedClaim("key-ttl"))

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			require.Len(t, cache.setNXTTLs, 1)
			tc.wantClaim(t, cache.setNXTTLs[0])
			require.Len(t, cache.setTTLs, 1, "the answer is stored once")
			assert.Equal(t, tc.ttl, cache.setTTLs[0], "storing the answer extends the key to the configured lifetime")
		})
	}
}
