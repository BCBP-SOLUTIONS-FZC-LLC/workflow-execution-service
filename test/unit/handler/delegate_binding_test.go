package handler_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	hhaReassignPath = "/api/v1/internal/workflows/reassign-delegate"
	hhaCancelPath   = "/api/v1/internal/workflows/cancel-by-delegate"
	hhaImpactPath   = "/api/v1/internal/workflows/delegate-impact"
)

func TestReassignDelegate_NilUUIDInRequiredID_Returns400(t *testing.T) {
	cases := []struct{ field string }{{"tenant_id"}, {"old_delegate_id"}, {"new_delegate_id"}}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			wc := &hhaWorkflowClient{}
			router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))
			body := map[string]any{
				"tenant_id":       uuid.New(),
				"old_delegate_id": uuid.New(),
				"new_delegate_id": uuid.New(),
			}
			body[tc.field] = uuid.Nil

			w := hhaPostJSON(router, hhaReassignPath, hhaJSONString(t, body))

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, tc.field, p.InvalidParams[0].Name)
			assert.Equal(t, "is required", p.InvalidParams[0].Reason)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, wc.reassignCalls, "a refused body must not reach the workflow client")
		})
	}
}

func TestCancelByDelegate_NilUUIDInRequiredID_Returns400(t *testing.T) {
	cases := []struct{ field string }{{"tenant_id"}, {"delegate_user_id"}}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			wc := &hhaWorkflowClient{}
			router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))
			body := map[string]any{"tenant_id": uuid.New(), "delegate_user_id": uuid.New()}
			body[tc.field] = uuid.Nil

			w := hhaPostJSON(router, hhaCancelPath, hhaJSONString(t, body))

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, tc.field, p.InvalidParams[0].Name)
			assert.Equal(t, "is required", p.InvalidParams[0].Reason)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, wc.cancelCalls, "a refused body must not reach the workflow client")
		})
	}
}

func TestCancelByDelegate_TextPlainContentType_Returns415(t *testing.T) {
	wc := &hhaWorkflowClient{}
	router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))
	body := hhaJSONString(t, map[string]any{"tenant_id": uuid.New(), "delegate_user_id": uuid.New()})

	w := hhaServe(router, http.MethodPost, hhaCancelPath, "text/plain", body)

	p := hhaDecodeProblem(t, w, http.StatusUnsupportedMediaType)
	assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", p.Code)
	assert.Equal(t, "request body must be application/json", p.Detail)
	assert.Empty(t, wc.cancelCalls, "a valid body under the wrong content type must not be processed")
}

// The detail text of a malformed UUID is deliberately not asserted here.
func TestCancelByDelegate_MalformedUUIDString_Returns400(t *testing.T) {
	cases := []struct{ field string }{{"tenant_id"}, {"delegate_user_id"}, {"delegation_id"}}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			wc := &hhaWorkflowClient{}
			router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))
			body := map[string]any{"tenant_id": uuid.New(), "delegate_user_id": uuid.New(), "delegation_id": uuid.New()}
			body[tc.field] = "not-a-uuid"

			w := hhaPostJSON(router, hhaCancelPath, hhaJSONString(t, body))

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, wc.cancelCalls, "a malformed id must not reach the workflow client")
		})
	}
}

func TestDelegateImpact_BothIDsInvalid_NamesTenantIDOnly(t *testing.T) {
	wc := &hhaWorkflowClient{}
	router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))

	w := hhaServe(router, http.MethodGet, hhaImpactPath+"?tenant_id=nope&delegate_user_id=also-nope", "", "")

	p := hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, "invalid tenant_id query parameter", p.Detail)
	assert.NotContains(t, w.Body.String(), "delegate_user_id")
	assert.Empty(t, wc.impactCalls)
}

func TestDelegateImpact_EmptyDelegationID_IsNoFilter(t *testing.T) {
	tenantID, delegateID := uuid.New(), uuid.New()
	flowA, flowB := uuid.New(), uuid.New()
	wc := &hhaWorkflowClient{impactResult: port.DelegateImpactResult{
		ActiveWorkflows: 2, ActiveAssignments: 3,
		WorkflowIDs: port.PageResult[uuid.UUID]{Items: []uuid.UUID{flowA, flowB}},
	}}
	router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))

	w := hhaServe(router, http.MethodGet,
		hhaImpactPath+"?tenant_id="+tenantID.String()+"&delegate_user_id="+delegateID.String()+"&delegation_id=", "", "")

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, wc.impactCalls, 1)
	got := wc.impactCalls[0]
	assert.Equal(t, tenantID, got.TenantID)
	assert.Equal(t, delegateID, got.DelegateUserID)
	assert.Nil(t, got.DelegationID, "an empty delegation_id must reach the client as no filter")
	var resp struct {
		ActiveWorkflows   int      `json:"active_workflows"`
		ActiveAssignments int      `json:"active_assignments"`
		WorkflowIDs       []string `json:"workflow_ids"`
	}
	hhaDecodeInto(t, w, &resp)
	assert.Equal(t, 2, resp.ActiveWorkflows)
	assert.Equal(t, 3, resp.ActiveAssignments)
	assert.Equal(t, []string{flowA.String(), flowB.String()}, resp.WorkflowIDs)
}

func TestDelegateImpact_NonCanonicalUUIDForms_AreAcceptedAndNormalised(t *testing.T) {
	tenantID, delegateID := uuid.New(), uuid.New()
	forms := []struct {
		name string
		form func(uuid.UUID) string
	}{
		{"uppercase", func(id uuid.UUID) string { return strings.ToUpper(id.String()) }},
		{"no hyphens", func(id uuid.UUID) string { return strings.ReplaceAll(id.String(), "-", "") }},
		{"urn prefix", func(id uuid.UUID) string { return "urn:uuid:" + id.String() }},
		{"braces", func(id uuid.UUID) string { return "{" + id.String() + "}" }},
	}
	for _, tc := range forms {
		for _, field := range []string{"tenant_id", "delegate_user_id"} {
			t.Run(tc.name+" "+field, func(t *testing.T) {
				wc := &hhaWorkflowClient{}
				router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))
				q := url.Values{"tenant_id": {tenantID.String()}, "delegate_user_id": {delegateID.String()}}
				switch field {
				case "tenant_id":
					q.Set(field, tc.form(tenantID))
				default:
					q.Set(field, tc.form(delegateID))
				}

				w := hhaServe(router, http.MethodGet, hhaImpactPath+"?"+q.Encode(), "", "")

				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Len(t, wc.impactCalls, 1)
				assert.Equal(t, tenantID, wc.impactCalls[0].TenantID)
				assert.Equal(t, delegateID, wc.impactCalls[0].DelegateUserID)
			})
		}
	}
}
