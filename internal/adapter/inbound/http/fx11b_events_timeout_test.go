package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func fx11bSlowOK(c *gin.Context) {
	time.Sleep(60 * time.Millisecond)
	c.Status(http.StatusOK)
}

func fx11bTimeoutEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestTimeout(20 * time.Millisecond))
	for _, path := range []string{"delegation", "user-profile", "tenant", "workflow-task"} {
		r.POST("/api/v1/internal/events/"+path, fx11bSlowOK)
	}
	r.POST("/api/v1/internal/other", fx11bSlowOK)
	return r
}

func TestFx11bEventRoutesAreExemptFromTheRequestTimeout(t *testing.T) {
	for _, path := range []string{"delegation", "user-profile", "tenant", "workflow-task"} {
		t.Run(path, func(t *testing.T) {
			w := httptest.NewRecorder()
			fx11bTimeoutEngine().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/internal/events/"+path, nil))

			assert.Equal(t, http.StatusOK, w.Code, "the handler's own answer stands once its effects committed")
		})
	}
}

func TestFx11bOtherRoutesKeepTheRequestTimeout(t *testing.T) {
	w := httptest.NewRecorder()
	fx11bTimeoutEngine().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/internal/other", nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
