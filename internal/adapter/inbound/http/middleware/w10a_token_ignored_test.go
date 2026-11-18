package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestRequireSystemCaller_InternalTokenHeaderIsIgnored_CallerWithIdentityIsAdmitted(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_internal_auth_rejected_total"
	rejected := func() float64 {
		return obstest.Sum(t, metric, map[string]string{"reason": "missing_user"}) + obstest.Sum(t, metric, map[string]string{"reason": "missing_role"})
	}
	cases := []struct {
		name  string
		token []string
	}{
		{"a wrong token", []string{"not-the-secret"}},
		{"an empty token", []string{""}},
		{"several tokens", []string{"one", "two"}},
		{"no token", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			r := gin.New()
			r.Use(middleware.RequireSystemCaller())
			reached := 0
			var seenUser string
			r.GET("/probe", func(c *gin.Context) {
				reached++
				seenUser = c.GetHeader("x-user-id")
				c.Status(http.StatusOK)
			})
			request := httptest.NewRequest(http.MethodGet, "/probe", nil)
			request.Header.Set("x-user-id", "iam-system")
			request.Header.Set("x-tenant-roles", "iam-system")
			for _, v := range tc.token {
				request.Header.Add("x-internal-token", v)
			}
			before := rejected()

			w := httptest.NewRecorder()
			r.ServeHTTP(w, request)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Equal(t, 1, reached, "the handler ran")
			assert.Equal(t, "iam-system", seenUser, "the handler sees the caller's identity unchanged")
			assert.Equal(t, before, rejected(), "an admitted caller is not counted as rejected")
		})
	}
}

func TestRequireSystemCaller_InternalTokenWithoutIdentity_IsRefusedAsMissingUser(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_internal_auth_rejected_total"
	count := func(reason string) float64 { return obstest.Sum(t, metric, map[string]string{"reason": reason}) }
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.RequireSystemCaller())
	reached := 0
	r.GET("/probe", func(c *gin.Context) {
		reached++
		c.Status(http.StatusOK)
	})
	request := httptest.NewRequest(http.MethodGet, "/probe", nil)
	request.Header.Set("x-internal-token", "any-token-at-all")
	missingUserBefore, missingRoleBefore := count("missing_user"), count("missing_role")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)

	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
	assert.Zero(t, reached, "the handler did not run")
	assert.Equal(t, missingUserBefore+1, count("missing_user"), "the refusal is counted as a missing user")
	assert.Equal(t, missingRoleBefore, count("missing_role"), "no other reason moves")
}
