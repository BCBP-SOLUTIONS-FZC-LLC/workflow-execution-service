package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
)

func newProbeRouter(token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.RequireInternalToken(token))
	r.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func TestRequireInternalToken_EmptyToken_PassesThrough(t *testing.T) {
	router := newProbeRouter("")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/probe", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireInternalToken_MatchingHeader_PassesThrough(t *testing.T) {
	router := newProbeRouter("secret-token")

	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	req.Header.Set(middleware.InternalTokenHeader, "secret-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireInternalToken_MissingHeader_401(t *testing.T) {
	router := newProbeRouter("secret-token")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/probe", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRequireInternalToken_MismatchedHeader_401(t *testing.T) {
	router := newProbeRouter("secret-token")

	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	req.Header.Set(middleware.InternalTokenHeader, "wrong-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
