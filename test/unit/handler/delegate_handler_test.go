package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	testOldDelegateID = uuid.MustParse("55555555-5555-5555-5555-555555555555")
	testNewDelegateID = uuid.MustParse("66666666-6666-6666-6666-666666666666")
	testDelegationID  = uuid.MustParse("77777777-7777-7777-7777-777777777777")
)

// --- ReassignDelegate ---

func TestReassignDelegate_Success(t *testing.T) {
	var gotIn port.ReassignDelegateInput
	fake := &fakeWorkflowClient{
		reassignDelegate: func(_ context.Context, in port.ReassignDelegateInput) (int, error) {
			gotIn = in
			return 7, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Reassigned int `json:"reassigned"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, 7, resp.Reassigned)
	assert.Equal(t, testTenantID, gotIn.TenantID)
	assert.Equal(t, testOldDelegateID, gotIn.OldDelegateID)
	assert.Equal(t, testNewDelegateID, gotIn.NewDelegateID)
}

// TestReassignDelegate_PartialFailure proves the DoD's partial-failure
// behavior at the port seam: the fake stands in for a real service that
// scope-filtered N active assignments down to a smaller eligible subset —
// the response's scalar count (per LLD §5.8/OpenAPI, no skipped[]/failed[]
// breakdown) is exactly that smaller number, not the full attempted set.
func TestReassignDelegate_PartialFailure(t *testing.T) {
	const attempted = 10
	const eligible = 7 // 3 rows held at old_delegate_id, ineligible for new_delegate_id
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return eligible, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Reassigned int `json:"reassigned"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, eligible, resp.Reassigned)
	assert.Less(t, resp.Reassigned, attempted)
}

func TestReassignDelegate_ZeroActiveTasks(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 0, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Reassigned int `json:"reassigned"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, 0, resp.Reassigned)
}

func TestReassignDelegate_DelegationIDOmitted(t *testing.T) {
	var gotIn port.ReassignDelegateInput
	fake := &fakeWorkflowClient{
		reassignDelegate: func(_ context.Context, in port.ReassignDelegateInput) (int, error) {
			gotIn = in
			return 1, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Nil(t, gotIn.DelegationID)
}

func TestReassignDelegate_DelegationIDPresent(t *testing.T) {
	var gotIn port.ReassignDelegateInput
	fake := &fakeWorkflowClient{
		reassignDelegate: func(_ context.Context, in port.ReassignDelegateInput) (int, error) {
			gotIn = in
			return 1, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
		"delegation_id":   testDelegationID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotIn.DelegationID)
	assert.Equal(t, testDelegationID, *gotIn.DelegationID)
}

func TestReassignDelegate_TenantMismatch(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 0, port.ErrTenantMismatch
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
		"delegation_id":   testDelegationID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusForbidden, w.Code)
	var resp struct {
		Code string `json:"code"`
		Type string `json:"type"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, "TENANT_MISMATCH", resp.Code)
	assert.Contains(t, resp.Type, "tenant-mismatch")
}

func TestReassignDelegate_MissingRequiredField(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			t.Fatal("service must not be called when binding fails")
			return 0, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		// new_delegate_id omitted
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestReassignDelegate_MalformedJSON(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{}))

	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", nil)
	r.Body = http.NoBody
	r.ContentLength = 0
	w := do(router, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestReassignDelegate_UpstreamUnavailable(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 0, adapterhttp.ErrUpstreamUnavailable
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", resp.Code)
}

func TestReassignDelegate_InternalError(t *testing.T) {
	fake := &fakeWorkflowClient{
		reassignDelegate: func(context.Context, port.ReassignDelegateInput) (int, error) {
			return 0, assertUnknownErr
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":       testTenantID,
		"old_delegate_id": testOldDelegateID,
		"new_delegate_id": testNewDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var resp struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, "INTERNAL_ERROR", resp.Code)
}

// --- CancelByDelegate ---

func TestCancelByDelegate_Success(t *testing.T) {
	var gotIn port.CancelByDelegateInput
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(_ context.Context, in port.CancelByDelegateInput) (int, error) {
			gotIn = in
			return 3, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Cancelled int `json:"cancelled"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, 3, resp.Cancelled)
	assert.Equal(t, testTenantID, gotIn.TenantID)
	assert.Equal(t, testOldDelegateID, gotIn.DelegateUserID)
}

func TestCancelByDelegate_PartialFailure(t *testing.T) {
	const eligible = 5
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
			return eligible, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Cancelled int `json:"cancelled"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, eligible, resp.Cancelled)
}

func TestCancelByDelegate_ZeroActiveTasks(t *testing.T) {
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
			return 0, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Cancelled int `json:"cancelled"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, 0, resp.Cancelled)
}

func TestCancelByDelegate_DelegationIDOmittedVsPresent(t *testing.T) {
	var gotIn port.CancelByDelegateInput
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(_ context.Context, in port.CancelByDelegateInput) (int, error) {
			gotIn = in
			return 1, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
	}))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Nil(t, gotIn.DelegationID)

	w = do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
		"delegation_id":    testDelegationID,
	}))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotIn.DelegationID)
	assert.Equal(t, testDelegationID, *gotIn.DelegationID)
}

func TestCancelByDelegate_TenantMismatch(t *testing.T) {
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
			return 0, port.ErrTenantMismatch
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
		"delegation_id":    testDelegationID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	require.Equal(t, http.StatusForbidden, w.Code)
	var resp struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, "TENANT_MISMATCH", resp.Code)
}

func TestCancelByDelegate_MissingRequiredField(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{
		cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
			t.Fatal("service must not be called when binding fails")
			return 0, nil
		},
	}))

	body := map[string]any{"tenant_id": testTenantID} // delegate_user_id omitted
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCancelByDelegate_MalformedJSON(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{}))

	r := internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", nil)
	r.Body = http.NoBody
	r.ContentLength = 0
	w := do(router, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCancelByDelegate_UpstreamUnavailable(t *testing.T) {
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
			return 0, adapterhttp.ErrUpstreamUnavailable
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestCancelByDelegate_InternalError(t *testing.T) {
	fake := &fakeWorkflowClient{
		cancelByDelegate: func(context.Context, port.CancelByDelegateInput) (int, error) {
			return 0, assertUnknownErr
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	body := map[string]any{
		"tenant_id":        testTenantID,
		"delegate_user_id": testOldDelegateID,
	}
	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body))

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- DelegateImpact ---

func TestDelegateImpact_Success(t *testing.T) {
	fake := &fakeWorkflowClient{
		delegateImpact: func(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			return port.DelegateImpactResult{
				ReassignedCount: 5,
				WorkflowIDs: port.PageResult[uuid.UUID]{
					Items:      []uuid.UUID{testInstID, testTaskID},
					NextCursor: "abc",
				},
			}, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		ReassignedCount int         `json:"reassigned_count"`
		WorkflowIDs     []uuid.UUID `json:"workflow_ids"`
		NextCursor      string      `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &resp)
	assert.Equal(t, 5, resp.ReassignedCount)
	assert.Equal(t, []uuid.UUID{testInstID, testTaskID}, resp.WorkflowIDs)
	assert.Equal(t, "abc", resp.NextCursor)
}

func TestDelegateImpact_ZeroImpact(t *testing.T) {
	fake := &fakeWorkflowClient{
		delegateImpact: func(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			return port.DelegateImpactResult{}, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	require.Equal(t, http.StatusOK, w.Code)
	// workflow_ids must serialize as [] not null (toDelegateImpactResp always
	// builds via make([]uuid.UUID, len(...))).
	assert.JSONEq(t, `{"reassigned_count":0,"workflow_ids":[]}`, w.Body.String())
}

func TestDelegateImpact_MissingTenantID(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{}))

	url := "/api/v1/internal/workflows/delegate-impact?delegate_user_id=" + testOldDelegateID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDelegateImpact_MissingDelegateUserID(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{}))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDelegateImpact_InvalidDelegationIDQuery(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{}))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() +
		"&delegate_user_id=" + testOldDelegateID.String() + "&delegation_id=not-a-uuid"
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDelegateImpact_InvalidLimit_Rejected(t *testing.T) {
	router := newInternalRouter(newDelegateHandler(&fakeWorkflowClient{}))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() +
		"&delegate_user_id=" + testOldDelegateID.String() + "&limit=-5"
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDelegateImpact_DelegationIDOmittedVsPresent(t *testing.T) {
	var gotIn port.DelegateImpactInput
	fake := &fakeWorkflowClient{
		delegateImpact: func(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			gotIn = in
			return port.DelegateImpactResult{}, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Nil(t, gotIn.DelegationID)

	url += "&delegation_id=" + testDelegationID.String()
	w = do(router, internalReq(http.MethodGet, url, nil))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotIn.DelegationID)
	assert.Equal(t, testDelegationID, *gotIn.DelegationID)
}

func TestDelegateImpact_TenantMismatch(t *testing.T) {
	fake := &fakeWorkflowClient{
		delegateImpact: func(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			return port.DelegateImpactResult{}, port.ErrTenantMismatch
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() +
		"&delegate_user_id=" + testOldDelegateID.String() + "&delegation_id=" + testDelegationID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestDelegateImpact_CursorAndLimitPassthrough(t *testing.T) {
	var gotIn port.DelegateImpactInput
	fake := &fakeWorkflowClient{
		delegateImpact: func(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			gotIn = in
			return port.DelegateImpactResult{}, nil
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() +
		"&delegate_user_id=" + testOldDelegateID.String() + "&cursor=xyz&limit=10"
	w := do(router, internalReq(http.MethodGet, url, nil))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "xyz", gotIn.Page.Cursor)
	assert.Equal(t, 10, gotIn.Page.Limit)
}

func TestDelegateImpact_UpstreamUnavailable(t *testing.T) {
	fake := &fakeWorkflowClient{
		delegateImpact: func(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			return port.DelegateImpactResult{}, adapterhttp.ErrUpstreamUnavailable
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestDelegateImpact_InternalError(t *testing.T) {
	fake := &fakeWorkflowClient{
		delegateImpact: func(context.Context, port.DelegateImpactInput) (port.DelegateImpactResult, error) {
			return port.DelegateImpactResult{}, assertUnknownErr
		},
	}
	router := newInternalRouter(newDelegateHandler(fake))

	url := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String()
	w := do(router, internalReq(http.MethodGet, url, nil))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
