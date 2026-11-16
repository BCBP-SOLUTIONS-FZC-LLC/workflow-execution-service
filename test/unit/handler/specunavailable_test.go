package handler_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func TestSpecUnavailable_Returns500WithFixedDetailAndRequestPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/docs/asyncapi.json", handler.SpecUnavailable)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/docs/asyncapi.json", nil))

	p := hrdDecodeProblem(t, w, http.StatusInternalServerError)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, "the AsyncAPI document is unavailable", p.Detail)
	assert.Equal(t, "/docs/asyncapi.json", p.Instance)
	assert.NotContains(t, w.Body.String(), "api/asyncapi.yaml", "the document's file path stays out of the body")
}
