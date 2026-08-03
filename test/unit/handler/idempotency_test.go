package handler_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

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
	}
	decodeJSON(t, w2.Body, &resp)
	assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", resp.Code)
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

	// bindJSON reads the same (now-erroring) body, so binding fails — but the
	// point of this test is that WithIdempotency's own drainBody bypass runs
	// the handler rather than panicking or hanging on the read error.
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, calls)
}

func TestIdempotency_CacheGetError_RunsHandlerUncached(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			calls++
			return 1, nil
		},
	}
	cache := newFakeCacheStore()
	cache.getErr = errors.New("cache transiently unavailable")
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
	assert.Equal(t, 1, calls, "a cache read error must not block the handler")
}

func TestIdempotency_CorruptedCacheEntry_RunsHandlerUncached(t *testing.T) {
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

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, calls, "an unparseable cache entry must not block the handler")
}
