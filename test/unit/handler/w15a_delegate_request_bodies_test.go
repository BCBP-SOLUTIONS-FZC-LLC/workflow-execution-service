package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	w15aReassignPath = "/api/v1/internal/workflows/reassign-delegate"
	w15aCancelPath   = "/api/v1/internal/workflows/cancel-by-delegate"
	w15aImpactPath   = "/api/v1/internal/workflows/delegate-impact"
)

type w15aParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type w15aProblem struct {
	Status        int         `json:"status"`
	Code          string      `json:"code"`
	Detail        string      `json:"detail"`
	InvalidParams []w15aParam `json:"invalid_params"`
}

// w15aDecodeProblem fails unless the response has the given status and a
// problem+json body, and returns that body decoded.
func w15aDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w15aProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w15aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

func w15aPost(router http.Handler, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func w15aGet(router http.Handler, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
	return w
}

func w15aJSON(t *testing.T, fields map[string]any) string {
	t.Helper()
	b, err := json.Marshal(fields)
	require.NoError(t, err)
	return string(b)
}

// w15aWorkflowClient records every input it receives; a nil value in a body
// field marshals to JSON null.
type w15aWorkflowClient struct {
	reassigned    int
	cancelled     int
	reassignCalls []port.ReassignDelegateInput
	cancelCalls   []port.CancelByDelegateInput
	impactCalls   []port.DelegateImpactInput
}

func (f *w15aWorkflowClient) ReassignDelegate(_ context.Context, in port.ReassignDelegateInput) (int, error) {
	f.reassignCalls = append(f.reassignCalls, in)
	return f.reassigned, nil
}

func (f *w15aWorkflowClient) CancelByDelegate(_ context.Context, in port.CancelByDelegateInput) (int, error) {
	f.cancelCalls = append(f.cancelCalls, in)
	return f.cancelled, nil
}

func (f *w15aWorkflowClient) DelegateImpact(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	f.impactCalls = append(f.impactCalls, in)
	return port.DelegateImpactResult{}, nil
}

var _ port.WorkflowClient = (*w15aWorkflowClient)(nil)

func w15aDelegateRouter(wc *w15aWorkflowClient) http.Handler {
	return newInternalRouter(handler.New(handler.Services{WorkflowClient: wc}))
}

func w15aReassignFields(tenant, oldDelegate, newDelegate uuid.UUID) map[string]any {
	return map[string]any{
		"tenant_id":       tenant.String(),
		"old_delegate_id": oldDelegate.String(),
		"new_delegate_id": newDelegate.String(),
	}
}

func w15aCancelFields(tenant, delegate uuid.UUID) map[string]any {
	return map[string]any{
		"tenant_id":        tenant.String(),
		"delegate_user_id": delegate.String(),
	}
}

func TestReassignDelegate_DelegationIDNullOrOmitted_PassesNoDelegation(t *testing.T) {
	cases := []struct {
		name         string
		explicitNull bool
	}{
		{name: "explicit null", explicitNull: true},
		{name: "omitted", explicitNull: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
			wc := &w15aWorkflowClient{reassigned: 4}
			fields := w15aReassignFields(tenant, oldDelegate, newDelegate)
			if tc.explicitNull {
				fields["delegation_id"] = nil
			}

			w := w15aPost(w15aDelegateRouter(wc), w15aReassignPath, w15aJSON(t, fields))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.JSONEq(t, `{"reassigned":4}`, w.Body.String())
			require.Len(t, wc.reassignCalls, 1)
			assert.Equal(t, port.ReassignDelegateInput{
				TenantID: tenant, OldDelegateID: oldDelegate, NewDelegateID: newDelegate, DelegationID: nil,
			}, wc.reassignCalls[0])
		})
	}
}

func TestCancelByDelegate_DelegationIDNullOrOmitted_PassesNoDelegation(t *testing.T) {
	cases := []struct {
		name         string
		explicitNull bool
	}{
		{name: "explicit null", explicitNull: true},
		{name: "omitted", explicitNull: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant, delegate := uuid.New(), uuid.New()
			wc := &w15aWorkflowClient{cancelled: 3}
			fields := w15aCancelFields(tenant, delegate)
			if tc.explicitNull {
				fields["delegation_id"] = nil
			}

			w := w15aPost(w15aDelegateRouter(wc), w15aCancelPath, w15aJSON(t, fields))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.JSONEq(t, `{"cancelled":3}`, w.Body.String())
			require.Len(t, wc.cancelCalls, 1)
			assert.Equal(t, port.CancelByDelegateInput{
				TenantID: tenant, DelegateUserID: delegate, DelegationID: nil,
			}, wc.cancelCalls[0])
		})
	}
}

func TestReassignDelegate_RequiredIDMissingOrNull_Returns400NamingTheField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   []w15aParam
	}{
		{"tenant_id missing", func(f map[string]any) { delete(f, "tenant_id") }, []w15aParam{{"tenant_id", "is required"}}},
		{"tenant_id null", func(f map[string]any) { f["tenant_id"] = nil }, []w15aParam{{"tenant_id", "is required"}}},
		{"old_delegate_id missing", func(f map[string]any) { delete(f, "old_delegate_id") }, []w15aParam{{"old_delegate_id", "is required"}}},
		{"old_delegate_id null", func(f map[string]any) { f["old_delegate_id"] = nil }, []w15aParam{{"old_delegate_id", "is required"}}},
		{"new_delegate_id missing", func(f map[string]any) { delete(f, "new_delegate_id") }, []w15aParam{{"new_delegate_id", "is required"}}},
		{"new_delegate_id null", func(f map[string]any) { f["new_delegate_id"] = nil }, []w15aParam{{"new_delegate_id", "is required"}}},
		{
			"both delegate ids missing",
			func(f map[string]any) { delete(f, "old_delegate_id"); delete(f, "new_delegate_id") },
			[]w15aParam{{"old_delegate_id", "is required"}, {"new_delegate_id", "is required"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &w15aWorkflowClient{}
			fields := w15aReassignFields(uuid.New(), uuid.New(), uuid.New())
			tc.mutate(fields)

			w := w15aPost(w15aDelegateRouter(wc), w15aReassignPath, w15aJSON(t, fields))

			p := w15aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.ElementsMatch(t, tc.want, p.InvalidParams)
			assert.Empty(t, wc.reassignCalls, "a refused body must not reach the workflow client")
		})
	}
}

func TestCancelByDelegate_RequiredIDMissingOrNull_Returns400NamingTheField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   []w15aParam
	}{
		{"tenant_id missing", func(f map[string]any) { delete(f, "tenant_id") }, []w15aParam{{"tenant_id", "is required"}}},
		{"tenant_id null", func(f map[string]any) { f["tenant_id"] = nil }, []w15aParam{{"tenant_id", "is required"}}},
		{"delegate_user_id missing", func(f map[string]any) { delete(f, "delegate_user_id") }, []w15aParam{{"delegate_user_id", "is required"}}},
		{"delegate_user_id null", func(f map[string]any) { f["delegate_user_id"] = nil }, []w15aParam{{"delegate_user_id", "is required"}}},
		{
			"both ids missing",
			func(f map[string]any) { delete(f, "tenant_id"); delete(f, "delegate_user_id") },
			[]w15aParam{{"tenant_id", "is required"}, {"delegate_user_id", "is required"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &w15aWorkflowClient{}
			fields := w15aCancelFields(uuid.New(), uuid.New())
			tc.mutate(fields)

			w := w15aPost(w15aDelegateRouter(wc), w15aCancelPath, w15aJSON(t, fields))

			p := w15aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.ElementsMatch(t, tc.want, p.InvalidParams)
			assert.Empty(t, wc.cancelCalls, "a refused body must not reach the workflow client")
		})
	}
}

func TestDelegateRoutes_NotJSONBody_Returns400NamingNoField(t *testing.T) {
	routes := []string{w15aReassignPath, w15aCancelPath}
	bodies := []struct{ name, body string }{
		{"truncated object", `{`},
		{"bare word", `abc`},
		{"empty body", ``},
	}
	for _, path := range routes {
		for _, b := range bodies {
			t.Run(strings.TrimPrefix(path, "/api/v1/internal/workflows/")+" "+b.name, func(t *testing.T) {
				wc := &w15aWorkflowClient{}

				w := w15aPost(w15aDelegateRouter(wc), path, b.body)

				p := w15aDecodeProblem(t, w, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, "request body is not valid JSON", p.Detail)
				var raw map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
				assert.NotContains(t, raw, "invalid_params", "a body that is not JSON names no field")
				assert.Empty(t, wc.reassignCalls)
				assert.Empty(t, wc.cancelCalls)
			})
		}
	}
}

// w15aPaddedBody returns a JSON body of exactly total bytes: the route's valid
// fields plus one extra string field that the route ignores.
func w15aPaddedBody(t *testing.T, fields map[string]any, total int) string {
	t.Helper()
	fields["padding"] = ""
	pad := total - len(w15aJSON(t, fields))
	require.Positive(t, pad)
	fields["padding"] = strings.Repeat("a", pad)
	body := w15aJSON(t, fields)
	require.Len(t, body, total)
	return body
}

func TestDelegateRoutes_BodySizeWithoutIdempotencyKey_RefusesOnlyOverTenMB(t *testing.T) {
	const tenMB = 10 * 1024 * 1024
	routes := []struct {
		name   string
		path   string
		fields func(tenant uuid.UUID) map[string]any
		// tenants lists the tenant of every call the client received.
		tenants func(*w15aWorkflowClient) []uuid.UUID
	}{
		{
			"reassign-delegate", w15aReassignPath,
			func(tenant uuid.UUID) map[string]any { return w15aReassignFields(tenant, uuid.New(), uuid.New()) },
			func(f *w15aWorkflowClient) []uuid.UUID {
				var out []uuid.UUID
				for _, c := range f.reassignCalls {
					out = append(out, c.TenantID)
				}
				return out
			},
		},
		{
			"cancel-by-delegate", w15aCancelPath,
			func(tenant uuid.UUID) map[string]any { return w15aCancelFields(tenant, uuid.New()) },
			func(f *w15aWorkflowClient) []uuid.UUID {
				var out []uuid.UUID
				for _, c := range f.cancelCalls {
					out = append(out, c.TenantID)
				}
				return out
			},
		},
	}
	sizes := []struct {
		name       string
		total      int
		wantStatus int
	}{
		{"exactly 10 MB is accepted", tenMB, http.StatusOK},
		{"one byte over 10 MB is refused", tenMB + 1, http.StatusRequestEntityTooLarge},
	}
	for _, rt := range routes {
		for _, sz := range sizes {
			t.Run(rt.name+" "+sz.name, func(t *testing.T) {
				wc := &w15aWorkflowClient{}
				tenant := uuid.New()
				body := w15aPaddedBody(t, rt.fields(tenant), sz.total)

				w := w15aPost(w15aDelegateRouter(wc), rt.path, body)

				if sz.wantStatus == http.StatusOK {
					require.Equal(t, http.StatusOK, w.Code)
					assert.Equal(t, []uuid.UUID{tenant}, rt.tenants(wc), "the bound tenant reaches the client")
					return
				}
				p := w15aDecodeProblem(t, w, http.StatusRequestEntityTooLarge)
				assert.Equal(t, "PAYLOAD_TOO_LARGE", p.Code)
				assert.Empty(t, p.InvalidParams)
				assert.Empty(t, rt.tenants(wc), "an oversized body must not be processed")
			})
		}
	}
}

func TestDelegateImpact_Limit_DefaultsToTwentyFiveAndClampsAtOneHundred(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantLimit int
	}{
		{"omitted", "", 25},
		{"inside the range", "&limit=7", 7},
		{"exactly 100", "&limit=100", 100},
		{"101", "&limit=101", 100},
		{"far above", "&limit=100000", 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant, delegate := uuid.New(), uuid.New()
			wc := &w15aWorkflowClient{}

			w := w15aGet(w15aDelegateRouter(wc),
				w15aImpactPath+"?tenant_id="+tenant.String()+"&delegate_user_id="+delegate.String()+tc.query)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, wc.impactCalls, 1)
			got := wc.impactCalls[0]
			assert.Equal(t, tenant, got.TenantID)
			assert.Equal(t, delegate, got.DelegateUserID)
			assert.Nil(t, got.Page.Cursor)
			assert.Equal(t, tc.wantLimit, got.Page.Limit, "limit "+strconv.Quote(tc.query))
		})
	}
}
