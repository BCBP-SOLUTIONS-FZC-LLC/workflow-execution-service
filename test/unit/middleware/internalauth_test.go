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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func newProbeRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.RequireSystemCaller())
	r.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func probe(r *gin.Engine, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestRequireSystemCaller(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantStatus int
	}{
		{"system caller passes", map[string]string{"x-user-id": "iam-system", "x-tenant-roles": "iam-system"}, http.StatusOK},
		{"role among several passes", map[string]string{"x-user-id": "u", "x-tenant-roles": "tenant_admin, iam-system"}, http.StatusOK},
		{"entries are trimmed", map[string]string{"x-user-id": "u", "x-tenant-roles": "  iam-system  ,x"}, http.StatusOK},
		{"no headers rejected", nil, http.StatusUnauthorized},
		{"missing user id rejected", map[string]string{"x-tenant-roles": "iam-system"}, http.StatusUnauthorized},
		{"blank user id rejected", map[string]string{"x-user-id": "   ", "x-tenant-roles": "iam-system"}, http.StatusUnauthorized},
		{"missing roles rejected", map[string]string{"x-user-id": "iam-system"}, http.StatusUnauthorized},
		{"other role rejected", map[string]string{"x-user-id": "u", "x-tenant-roles": "tenant_admin"}, http.StatusUnauthorized},
		{"role match is case sensitive", map[string]string{"x-user-id": "u", "x-tenant-roles": "IAM-System"}, http.StatusUnauthorized},
		{"role match is exact", map[string]string{"x-user-id": "u", "x-tenant-roles": "iam-system-x,not-iam-system"}, http.StatusUnauthorized},
		{"the old token header alone is rejected", map[string]string{"x-internal-token": "secret"}, http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := probe(newProbeRouter(), tt.headers)

			require.Equal(t, tt.wantStatus, w.Code)
			if tt.wantStatus == http.StatusOK {
				return
			}
			assertProblemBody(t, w)
		})
	}
}

// assertProblemBody checks the 401 carries the documented RFC-9457 problem
// with the caller-identity detail.
func assertProblemBody(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	var body struct {
		Code   string `json:"code"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "UNAUTHORIZED", body.Code)
	assert.Equal(t, http.StatusUnauthorized, body.Status)
	assert.Equal(t, "missing or invalid internal caller identity", body.Detail)
}

func TestRequireSystemCaller_TheRefusalCarriesTheCorrelationIds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Header("X-Trace-ID", "trace-abc")
		c.Header("X-Request-ID", "request-xyz")
	})
	r.Use(middleware.RequireSystemCaller())
	r.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/probe", nil))

	require.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "trace-abc", body["trace_id"])
	assert.Equal(t, "request-xyz", body["request_id"])
}

// A rejected caller is counted by reason, so a wrong mesh principal or a
// service that dropped its identity headers shows up as a rate and not only
// as 401s mixed into the HTTP error ratio.
func TestRequireSystemCaller_CountsRejectionsByReason(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_internal_auth_rejected_total"
	count := func(reason string) float64 { return obstest.Sum(t, metric, map[string]string{"reason": reason}) }

	cases := []struct {
		name    string
		headers map[string]string
		reason  string
	}{
		{"no user id", map[string]string{"x-tenant-roles": "iam-system"}, "missing_user"},
		{"blank user id", map[string]string{"x-user-id": "  ", "x-tenant-roles": "iam-system"}, "missing_user"},
		{"nothing at all names the user first", nil, "missing_user"},
		{"no role", map[string]string{"x-user-id": "u"}, "missing_role"},
		{"another role", map[string]string{"x-user-id": "u", "x-tenant-roles": "tenant_admin"}, "missing_role"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, otherBefore := count(tc.reason), count("missing_user")+count("missing_role")-count(tc.reason)

			w := probe(newProbeRouter(), tc.headers)

			require.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Equal(t, before+1, count(tc.reason))
			assert.Equal(t, otherBefore, count("missing_user")+count("missing_role")-count(tc.reason), "only the matching reason moves")
		})
	}

	t.Run("an admitted caller is not counted", func(t *testing.T) {
		before := count("missing_user") + count("missing_role")
		w := probe(newProbeRouter(), map[string]string{"x-user-id": "iam-system", "x-tenant-roles": "iam-system"})
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, before, count("missing_user")+count("missing_role"))
	})
}

// The identity every sender names and the middleware requires is one value.
func TestSystemCallerIdentity_IsOneConstant(t *testing.T) {
	assert.Equal(t, "iam-system", domain.SystemCaller)
}
