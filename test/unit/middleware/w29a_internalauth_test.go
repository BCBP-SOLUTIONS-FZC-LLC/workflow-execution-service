package middleware_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func w29aProbe(path string, headers map[string]string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.RequireSystemCaller())
	r.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// Serial: it reads the rejection counter before and after each call.
func TestRequireSystemCaller_RoleListWithBlankEntries_IsSplitOnCommaAndTheSystemRoleFound(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_internal_auth_rejected_total"
	rejected := func() float64 {
		return obstest.Sum(t, metric, map[string]string{"reason": "missing_role"})
	}

	cases := []struct {
		name       string
		roles      string
		wantStatus int
	}{
		{name: "spaces around entries and a trailing empty entry", roles: " a , iam-system , ", wantStatus: http.StatusOK},
		{name: "leading empty entries", roles: ",,iam-system", wantStatus: http.StatusOK},
		{name: "the system role is the only entry, padded", roles: "   iam-system   ", wantStatus: http.StatusOK},
		{name: "only empty entries", roles: ", ,", wantStatus: http.StatusUnauthorized},
		{name: "an empty header", roles: "", wantStatus: http.StatusUnauthorized},
		{name: "a blank header", roles: "   ", wantStatus: http.StatusUnauthorized},
		{name: "the role inside another entry is not found", roles: "x iam-system,iam-system y", wantStatus: http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := rejected()

			w := w29aProbe("/probe", map[string]string{"x-user-id": "u", "x-tenant-roles": tc.roles})

			require.Equal(t, tc.wantStatus, w.Code)
			if tc.wantStatus == http.StatusOK {
				assert.Equal(t, before, rejected(), "an admitted caller is not counted")
				return
			}
			assert.Equal(t, before+1, rejected(), "refused as a missing role")
			var body map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, "UNAUTHORIZED", body["code"])
		})
	}
}

// A refusal that no observability middleware ran in front of has no
// correlation ids to copy: the body leaves both fields out, and the instance is the
// path without its query.
func TestRequireSystemCaller_RefusalOutsideTheObservabilityMiddlewares_OmitsTheCorrelationIds(t *testing.T) {
	w := w29aProbe("/probe?token=abc&x=1", nil)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	assert.Empty(t, w.Header().Get("X-Trace-ID"))
	assert.Empty(t, w.Header().Get("X-Request-ID"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotContains(t, body, "trace_id")
	assert.NotContains(t, body, "request_id")
	assert.Equal(t, "/probe", body["instance"], "the path only, without the query")
	assert.Equal(t, "UNAUTHORIZED", body["code"])
	assert.Equal(t, "Unauthorized", body["title"])
	assert.EqualValues(t, http.StatusUnauthorized, body["status"])
	assert.Equal(t, "missing or invalid internal caller identity", body["detail"])
	assert.NotContains(t, w.Body.String(), "abc", "the query string stays out of the answer")
}
