package handler_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// errReader is an io.ReadCloser whose Read always fails, simulating a
// request-body read error (e.g. a client disconnect mid-upload).
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("simulated read error") }
func (errReader) Close() error             { return nil }

func TestIdempotency_NoHeaderNilCache_RunsNormally(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	// newDelegateHandler leaves Cache nil — WithIdempotency must no-op.
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, calls)
}

func TestIdempotency_FirstCall_CachesResponse(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 5, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, calls)
	assert.NotEmpty(t, cache.data)
}

func TestIdempotency_ReplaySameBody_ReturnsCachedResponse(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 5, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}

	r1 := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r1.Header.Set("Idempotency-Key", "key-1")
	w1 := do(router, r1)
	require.Equal(t, http.StatusOK, w1.Code)

	r2 := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r2.Header.Set("Idempotency-Key", "key-1")
	w2 := do(router, r2)

	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 1, calls, "handler must not be re-invoked on a matching replay")
}

func TestIdempotency_ReplayDifferentBody_409(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 5, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body1 := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r1 := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body1)
	r1.Header.Set("Idempotency-Key", "key-1")
	w1 := do(router, r1)
	require.Equal(t, http.StatusOK, w1.Code)

	body2 := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testOldDelegateID, // different body
	}
	r2 := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body2)
	r2.Header.Set("Idempotency-Key", "key-1")
	w2 := do(router, r2)

	require.Equal(t, http.StatusConflict, w2.Code)
	var resp struct {
		Code string `json:"code"`
		Type string `json:"type"`
	}
	decodeJSON(t, w2.Body, &resp)
	assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", resp.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/idempotency-key-replay", resp.Type, "must use its own dedicated type URI, not the generic conflict one")
}

func TestIdempotency_NonSuccessResponse_NotCached(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 0, port.ErrTenantMismatch
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}

	r1 := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r1.Header.Set("Idempotency-Key", "key-1")
	w1 := do(router, r1)
	require.Equal(t, http.StatusForbidden, w1.Code)

	r2 := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r2.Header.Set("Idempotency-Key", "key-1")
	w2 := do(router, r2)
	require.Equal(t, http.StatusForbidden, w2.Code)

	assert.Equal(t, 2, calls, "a non-2xx response must not be cached — a replay re-executes the handler")
}

func TestIdempotency_BodyReadError_BypassesProtection(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", nil)
	r.Body = errReader{}
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	// drainBody's own read error is mapped straight to a response (via
	// bindErrResponse) without ever invoking the wrapped handler.
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, calls)
}

// TestIdempotency_OversizedBody_RejectedNotProcessed is a regression test:
// drainBody must cap its read at maxBodyBytes just like the wrapped
// handler's own bindJSON does, so an oversized body can never be buffered in
// full before the size floor gets a chance to reject it — and the rejection
// itself must be the same clean 413 every other size-capped route returns,
// not a generic 400 from a downstream JSON parse failure on a body that
// drainBody already partially consumed.
func TestIdempotency_OversizedBody_RejectedNotProcessed(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
		"padding":         strings.Repeat("a", 11<<20), // over the 10 MB cap
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, "must be the same clean 413 every size-capped route returns")
	var resp problemBody
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, "PAYLOAD_TOO_LARGE", resp.Code)
	assert.Equal(t, 0, calls)
}

func TestIdempotency_CacheSetNXError_RunsHandlerUncached(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	cache.setNXErr = errors.New("cache transiently unavailable")
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, calls, "a cache claim error must not block the handler")
}

// TestIdempotency_CacheSetNXError_LogsWarn is a regression test: WithIdempotency
// used to have no way to log a cache failure at all (its own doc comment
// claimed "execution_service has no port.Logger abstraction yet", which was
// already stale — port.Logger existed elsewhere in the same commit set).
func TestIdempotency_CacheSetNXError_LogsWarn(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	cache.setNXErr = errors.New("cache transiently unavailable")
	var warned bool
	log := &fakeLogger{warn: func(string, map[string]any) { warned = true }}
	router := newInternalRouter(newDelegateHandlerWithCacheAndLog(fake, cache, log))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, warned, "a cache claim error must be logged at WARN, not silently swallowed")
}

// TestIdempotency_CorruptedCacheEntry_RejectsReplay: SetNX correctly reports
// the key as already claimed (the entry exists), but the entry itself is
// unreadable — the safe behavior is to reject rather than risk running the
// handler a second time for a key that might genuinely be mid-flight
// elsewhere.
func TestIdempotency_CorruptedCacheEntry_RejectsReplay(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")

	// Seed a cache entry that isn't valid JSON under the exact key
	// WithIdempotency will compute, simulating a corrupted/foreign entry.
	cache.data["idem:POST:/api/v1/internal/workflows/reassign-delegate:key-1"] = "not-json"

	w := do(router, r)

	require.Equal(t, http.StatusConflict, w.Code)
	assert.Equal(t, 0, calls, "an unreadable claimed key must not also let the handler run")
}

// TestIdempotency_ConcurrentSameKey_SecondRequestRejected is a regression
// test for the race WithIdempotency used to have: a plain Get-then-Set with
// no atomic claim let two requests sharing an Idempotency-Key both miss the
// cache and both execute the handler. SetNX now closes that window — a
// request arriving while another is still mid-flight for the same key (here
// simulated by pre-seeding the claim sentinel) must be rejected outright,
// never allowed to also invoke the handler.
func TestIdempotency_ConcurrentSameKey_SecondRequestRejected(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(fake, cache))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")

	// Simulate a concurrent in-flight request already holding the claim.
	cache.data["idem:POST:/api/v1/internal/workflows/reassign-delegate:key-1"] = "CLAIMED"

	w := do(router, r)

	require.Equal(t, http.StatusConflict, w.Code)
	assert.Equal(t, 0, calls, "a request racing an in-flight claim must not also execute the handler")
}

// TestIdempotency_ClaimTask_ReplaySameBody_ReturnsCachedResponse guards
// against a regression on /api/v1/tasks/:id/claim specifically: it was
// registered bare (no WithIdempotency) despite the OpenAPI spec documenting
// Idempotency-Key support for it, same as every other mutating endpoint.
// TestIdempotency_ClaimedKeyGetError_RejectsReplay exercises replayIfCached's
// own cache.Get failure path: SetNX correctly reports the key as claimed,
// but the follow-up Get needed to read what's there fails transiently. The
// safe behavior is still to reject rather than risk a second execution.
func TestIdempotency_ClaimedKeyGetError_RejectsReplay(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	cache.data["idem:POST:/api/v1/internal/workflows/reassign-delegate:key-1"] = "CLAIMED"
	var warned bool
	log := &fakeLogger{warn: func(string, map[string]any) { warned = true }}
	router := newInternalRouter(newDelegateHandlerWithCacheAndLog(fake, cache, log))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")

	// The claim already exists (above), so WithIdempotency calls
	// replayIfCached, whose Get call fails here.
	cache.getErr = errors.New("cache transiently unavailable")
	w := do(router, r)

	require.Equal(t, http.StatusConflict, w.Code)
	assert.Equal(t, 0, calls)
	assert.True(t, warned, "a Get failure on an already-claimed key must be logged at WARN")
}

// TestIdempotency_SetAfterSuccessFails_LogsWarn: storeResult's cache.Set call
// (replacing the claim with the real response) failing must not affect the
// response already sent to the caller — just be logged.
func TestIdempotency_SetAfterSuccessFails_LogsWarn(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 1, nil
		},
	}
	cache := &setFailingCacheStore{fakeCacheStore: newFakeCacheStore()}
	var warned bool
	log := &fakeLogger{warn: func(string, map[string]any) { warned = true }}
	router := newInternalRouter(newDelegateHandlerWithCacheAndLog(fake, cache, log))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code, "the response already computed must still be sent even if caching it fails")
	assert.True(t, warned, "a Set failure after a successful response must be logged at WARN")
}

// TestIdempotency_DelAfterFailureFails_LogsWarn: storeResult's cache.Del call
// (releasing the claim after a non-2xx response) failing must not affect the
// response already sent to the caller — just be logged.
func TestIdempotency_DelAfterFailureFails_LogsWarn(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 0, port.ErrTenantMismatch
		},
	}
	cache := &delFailingCacheStore{fakeCacheStore: newFakeCacheStore()}
	var warned bool
	log := &fakeLogger{warn: func(string, map[string]any) { warned = true }}
	router := newInternalRouter(newDelegateHandlerWithCacheAndLog(fake, cache, log))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	r.Header.Set("Idempotency-Key", "key-1")
	w := do(router, r)

	require.Equal(t, http.StatusForbidden, w.Code, "the response already computed must still be sent even if releasing the claim fails")
	assert.True(t, warned, "a Del failure after a non-2xx response must be logged at WARN")
}

func TestIdempotency_ClaimTask_ReplaySameBody_ReturnsCachedResponse(t *testing.T) {
	calls := 0
	fake := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			calls++
			return &port.Task{ID: testTaskID, RecordVersion: 1}, nil
		},
	}
	cache := newFakeCacheStore()
	router := newRouter(newHandlerWithCache(fake, &fakeEligibilityChecker{}, cache))

	body := map[string]any{"record_version": 1}

	r1 := req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", body)
	r1.Header.Set("Idempotency-Key", "claim-key-1")
	w1 := do(router, r1)
	require.Equal(t, http.StatusAccepted, w1.Code)

	r2 := req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/claim", body)
	r2.Header.Set("Idempotency-Key", "claim-key-1")
	w2 := do(router, r2)

	require.Equal(t, http.StatusAccepted, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 1, calls, "a safe retry with the same Idempotency-Key must not re-invoke the handler")
}
