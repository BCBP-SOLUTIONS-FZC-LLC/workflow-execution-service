package handler_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w25aClaimBody = `{"record_version":1}`

func w25aClaimPath() string { return "/api/v1/tasks/" + testTaskID.String() + "/claim" }

func w25aClaimCacheKey(key string) string {
	return "idem:" + testTenantID.String() + ":" + testUserID.String() + ":POST:" + w25aClaimPath() + ":" + key
}

func w25aKeyedClaim(key string) *http.Request {
	r := w25aRaw(http.MethodPost, w25aClaimPath(), "application/json", w25aClaimBody)
	r.Header.Set("Idempotency-Key", key)
	return r
}

type w25aStoredAnswer struct {
	Status   int    `json:"status"`
	Body     []byte `json:"body"`
	BodyHash string `json:"body_hash"`
}

// A first keyed call claims the scoped key with the claim marker and the configured lifetime (24 hours when none
// is configured), runs the handler once, and stores the status, the answer body and the request body hash under
// the same key.
func TestIdempotency_FirstKeyedCall_ClaimsScopedKeyThenStoresAnswer(t *testing.T) {
	cases := []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration
	}{
		{"configured lifetime is used", 90 * time.Minute, 90 * time.Minute},
		{"no configured lifetime defaults to 24 hours", 0, 24 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := w25aNewCache()
			var claimed []uuid.UUID
			tasks := &fakeTaskService{
				claim: func(_ context.Context, tenantID, taskID, userID uuid.UUID, version int64) (*port.Task, error) {
					assert.Equal(t, testTenantID, tenantID)
					assert.Equal(t, testUserID, userID)
					assert.Equal(t, int64(1), version)
					claimed = append(claimed, taskID)
					return newTestTask(), nil
				},
			}
			router := newRouter(handler.New(handler.Services{Tasks: tasks, Eligibility: &fakeEligibilityChecker{}, Cache: cache, IdempotencyTTL: tc.ttl}))

			w := w25aServe(router, w25aKeyedClaim("key-1"))

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			assert.Equal(t, []uuid.UUID{testTaskID}, claimed, "the handler runs once")
			wantKey := w25aClaimCacheKey("key-1")
			assert.Equal(t, []string{wantKey}, cache.setNXKeys)
			assert.Equal(t, []string{"CLAIMED"}, cache.setNXVals)
			assert.Equal(t, []time.Duration{time.Minute}, cache.setNXTTLs, "the claim is short whatever the answer lifetime")

			require.Equal(t, []string{wantKey}, cache.setKeys, "the answer replaces the claim under the same key")
			assert.Equal(t, []time.Duration{tc.wantTTL}, cache.setTTLs, "the stored answer takes the configured lifetime")
			assert.Empty(t, cache.delKeys, "a stored answer releases nothing")
			var stored w25aStoredAnswer
			require.NoError(t, json.Unmarshal([]byte(cache.setVals[0]), &stored))
			sum := sha256.Sum256([]byte(w25aClaimBody))
			assert.Equal(t, http.StatusAccepted, stored.Status)
			assert.Equal(t, w.Body.Bytes(), stored.Body, "the stored body is the body the caller received")
			assert.Equal(t, hex.EncodeToString(sum[:]), stored.BodyHash)
		})
	}
}

// A second request that finds the key held with an empty entry, an unreadable entry or a failing cache read is
// refused with 409 IDEMPOTENCY_KEY_REPLAY and never reaches the handler. A cache read error or an unreadable entry
// is also logged as a warning carrying the cache key (and is safe with no logger); an empty entry logs nothing.
func TestIdempotency_HeldKeyWithoutUsableAnswer_RefusesWithReplayCode(t *testing.T) {
	cases := []struct {
		name     string
		seed     *string
		getErr   error
		logger   bool
		wantWarn int
	}{
		{name: "empty stored entry", seed: w25aPtr(""), logger: true, wantWarn: 0},
		{name: "stored entry is not JSON", seed: w25aPtr("not-json"), logger: true, wantWarn: 1},
		{name: "cache read fails", seed: w25aPtr("CLAIMED"), getErr: errors.New("cache down"), logger: true, wantWarn: 1},
		{name: "stored entry is not JSON and no logger is configured", seed: w25aPtr("not-json"), logger: false, wantWarn: 0},
		{name: "cache read fails and no logger is configured", seed: w25aPtr("CLAIMED"), getErr: errors.New("cache down"), logger: false, wantWarn: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := w25aNewCache()
			cache.data[w25aClaimCacheKey("key-1")] = *tc.seed
			cache.getErr = tc.getErr
			log := &w25aLog{}
			svc := handler.Services{Tasks: &fakeTaskService{
				claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
					t.Error("the handler must not run for a held key")
					return newTestTask(), nil
				},
			}, Eligibility: &fakeEligibilityChecker{}, Cache: cache, IdempotencyTTL: time.Hour}
			if tc.logger {
				svc.Log = log
			}
			router := newRouter(handler.New(svc))

			w := w25aServe(router, w25aKeyedClaim("key-1"))

			p := w25aDecodeProblem(t, w, http.StatusConflict)
			assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", p.Code)
			assert.Equal(t, "https://errors.bcbp.io/execution/idempotency-key-replay", p.Type)
			assert.Equal(t, w25aClaimPath(), p.Instance)
			assert.Empty(t, cache.setKeys, "a refused request stores nothing")
			require.Len(t, log.warns, tc.wantWarn)
			for _, warn := range log.warns {
				assert.Equal(t, w25aClaimCacheKey("key-1"), warn.fields["cache_key"])
				assert.NotEmpty(t, warn.fields["error"])
			}
		})
	}
}

func w25aPtr(s string) *string { return &s }

// While the first request with a key is still inside its handler, a second request with the same key is refused
// with 409 IDEMPOTENCY_KEY_REPLAY without running the handler; once the first finishes, a third gets its stored answer.
func TestIdempotency_SecondRequestWhileFirstIsRunning_IsRefusedUntilTheFirstStoresItsAnswer(t *testing.T) {
	cache := w25aNewCache()
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFirst)
	var runs atomic.Int32
	tasks := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			// Only the first run waits, so a second run that leaks past the key fails the test instead of hanging it.
			if runs.Add(1) == 1 {
				close(entered)
				<-release
			}
			return newTestTask(), nil
		},
	}
	router := newRouter(handler.New(handler.Services{Tasks: tasks, Eligibility: &fakeEligibilityChecker{}, Cache: cache, IdempotencyTTL: time.Hour}))

	firstDone := make(chan *http.Response, 1)
	go func() {
		w := w25aServe(router, w25aKeyedClaim("key-1"))
		firstDone <- w.Result()
	}()
	select {
	case <-entered:
	case first := <-firstDone:
		t.Fatalf("the first request finished (status %d) without reaching its handler", first.StatusCode)
	}

	second := w25aServe(router, w25aKeyedClaim("key-1"))
	p := w25aDecodeProblem(t, second, http.StatusConflict)
	assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/idempotency-key-replay", p.Type)
	require.EqualValues(t, 1, runs.Load(), "the second request did not run the handler")

	releaseFirst()
	first := <-firstDone
	assert.Equal(t, http.StatusAccepted, first.StatusCode)

	third := w25aServe(router, w25aKeyedClaim("key-1"))
	assert.Equal(t, http.StatusAccepted, third.Code, third.Body.String())
	assert.EqualValues(t, 1, runs.Load(), "the handler ran only for the first request")
}
