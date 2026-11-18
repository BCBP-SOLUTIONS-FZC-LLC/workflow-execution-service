package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
)

func w10bServeProblem(t *testing.T, headers map[string]string, handlers ...gin.HandlerFunc) (map[string]any, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Header("X-Trace-ID", "w10b-trace")
		c.Header("X-Request-ID", "w10b-request")
	})
	r.POST("/api/v1/internal/probe", handlers...)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/probe", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body), w.Body.String())
	return body, w.Header().Get("Content-Type")
}

func TestRequireSystemCaller_UnauthorizedBody_MatchesTheHandlerProblemWriter(t *testing.T) {
	const detail = "missing or invalid internal caller identity"
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{name: "no identity headers (missing user)", headers: nil},
		{name: "user id without the system role (missing role)", headers: map[string]string{"x-user-id": "w10b-user", "x-tenant-roles": "tenant_admin"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fromMiddleware, middlewareType := w10bServeProblem(t, tc.headers, middleware.RequireSystemCaller(), func(c *gin.Context) { c.Status(http.StatusOK) })
			fromWriter, writerType := w10bServeProblem(t, tc.headers, func(c *gin.Context) {
				writeProblem(c, http.StatusUnauthorized, CodeUnauthorized, detail, nil)
			})

			assert.Equal(t, detail, fromMiddleware["detail"])
			assert.Equal(t, fromWriter, fromMiddleware, "same type, title, status, detail, instance, code and correlation ids")
			assert.Equal(t, writerType, middlewareType)
			assert.Equal(t, "application/problem+json", middlewareType)
			assert.Equal(t, "UNAUTHORIZED", fromMiddleware["code"])
			assert.Equal(t, "/api/v1/internal/probe", fromMiddleware["instance"])
			assert.Equal(t, "w10b-trace", fromMiddleware["trace_id"])
			assert.Equal(t, "w10b-request", fromMiddleware["request_id"])
		})
	}
}
