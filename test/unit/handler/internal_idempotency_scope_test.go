package handler_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func internalCancelRequest(tenantID uuid.UUID, key string) *http.Request {
	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", map[string]any{
		"tenant_id":        tenantID,
		"delegate_user_id": testOldDelegateID,
	})
	r.Header.Set("Idempotency-Key", key)
	return r
}

func TestInternalIdempotency_TwoTenantsMayReuseOneKey(t *testing.T) {
	var tenants []uuid.UUID
	fake := &fakeWorkflowClient{cancelByDelegate: func(_ context.Context, in port.CancelByDelegateInput) (int, error) {
		tenants = append(tenants, in.TenantID)
		return 1, nil
	}}
	router := newInternalRouter(newDelegateHandlerWithCache(fake, newFakeCacheStore()))
	tenantA, tenantB := uuid.New(), uuid.New()

	first := do(router, internalCancelRequest(tenantA, "shared-key"))
	second := do(router, internalCancelRequest(tenantB, "shared-key"))

	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	assert.Equal(t, []uuid.UUID{tenantA, tenantB}, tenants)
}

func TestInternalIdempotency_ARepeatInTheSameTenantReplaysTheAnswer(t *testing.T) {
	calls := 0
	fake := &fakeWorkflowClient{cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
		calls++
		return 4, nil
	}}
	router := newInternalRouter(newDelegateHandlerWithCache(fake, newFakeCacheStore()))
	tenant := uuid.New()

	first := do(router, internalCancelRequest(tenant, "repeat-key"))
	second := do(router, internalCancelRequest(tenant, "repeat-key"))

	require.Equal(t, http.StatusOK, second.Code)
	assert.JSONEq(t, first.Body.String(), second.Body.String())
	assert.Equal(t, 1, calls)
}

func TestInternalIdempotency_TheStoredKeyNamesTheBodyTenantAndTheSystemCaller(t *testing.T) {
	cache := newFakeCacheStore()
	router := newInternalRouter(newDelegateHandlerWithCache(&fakeWorkflowClient{cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
		return 1, nil
	}}, cache))
	tenant := uuid.New()

	require.Equal(t, http.StatusOK, do(router, internalCancelRequest(tenant, "scoped-key")).Code)

	require.Len(t, cache.data, 1)
	for key := range cache.data {
		assert.True(t, strings.HasPrefix(key, "idem:"+tenant.String()+":"+domain.SystemCaller+":"), key)
	}
}
