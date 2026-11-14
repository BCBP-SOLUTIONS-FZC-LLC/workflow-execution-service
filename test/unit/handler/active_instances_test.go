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
)

func hhaActiveInstancesPath(workflowID uuid.UUID) string {
	return "/api/v1/internal/workflows/" + workflowID.String() + "/active-instances"
}

type hhaActiveBody struct {
	HasActive bool  `json:"has_active"`
	Count     int32 `json:"count"`
}

func TestActiveInstances_NonCanonicalTenantForms_CountTheSameTenant(t *testing.T) {
	tenantID, workflowID := uuid.New(), uuid.New()
	forms := map[string]string{
		"uppercase":  strings.ToUpper(tenantID.String()),
		"no hyphens": strings.ReplaceAll(tenantID.String(), "-", ""),
		"urn prefix": "urn:uuid:" + tenantID.String(),
		"braces":     "{" + tenantID.String() + "}",
	}
	for name, form := range forms {
		t.Run(name, func(t *testing.T) {
			guard := &hhaArchiveGuard{active: map[hhaTenantWorkflow]int32{{tenantID, workflowID}: 4}}
			router := hhaInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))
			q := url.Values{"tenant_id": {form}}

			w := hhaServe(router, http.MethodGet, hhaActiveInstancesPath(workflowID)+"?"+q.Encode(), "", "")

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.JSONEq(t, `{"has_active":true,"count":4}`, w.Body.String())
			assert.Equal(t, []hhaTenantWorkflow{{tenantID, workflowID}}, guard.calls)
		})
	}
}

func TestActiveInstances_RepeatedTenantID_FirstValueDecides(t *testing.T) {
	tenantA, tenantB, workflowID := uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name       string
		first      uuid.UUID
		second     uuid.UUID
		wantCount  int32
		wantTenant uuid.UUID
	}{
		{"A then B", tenantA, tenantB, 2, tenantA},
		{"B then A", tenantB, tenantA, 9, tenantB},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guard := &hhaArchiveGuard{active: map[hhaTenantWorkflow]int32{
				{tenantA, workflowID}: 2,
				{tenantB, workflowID}: 9,
			}}
			router := hhaInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))

			w := hhaServe(router, http.MethodGet,
				hhaActiveInstancesPath(workflowID)+"?tenant_id="+tc.first.String()+"&tenant_id="+tc.second.String(), "", "")

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var got hhaActiveBody
			hhaDecodeInto(t, w, &got)
			assert.Equal(t, hhaActiveBody{HasActive: true, Count: tc.wantCount}, got)
			assert.Equal(t, []hhaTenantWorkflow{{tc.wantTenant, workflowID}}, guard.calls, "only the first tenant may be counted")
		})
	}
}

func TestActiveInstances_TenantIDNotAUUID_Returns400(t *testing.T) {
	guard := &hhaArchiveGuard{}
	router := hhaInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))

	w := hhaServe(router, http.MethodGet, hhaActiveInstancesPath(uuid.New())+"?tenant_id=not-a-uuid", "", "")

	p := hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, "tenant_id must be a UUID", p.Detail)
	hhaAssertNoInternalText(t, w)
	assert.Empty(t, guard.calls, "a refused tenant_id must not reach the guard")
}
