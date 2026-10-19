package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

const InternalTokenHeader = "x-internal-token"

type problemDetails struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Instance  string `json:"instance"`
	Code      string `json:"code"`
	TraceID   string `json:"trace_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func RequireInternalToken(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.Next()
			return
		}
		provided := c.GetHeader(InternalTokenHeader)
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			c.Header("Content-Type", "application/problem+json")
			c.AbortWithStatusJSON(http.StatusUnauthorized, problemDetails{
				Type:      "https://errors.bcbp.io/execution/unauthorized",
				Title:     "Unauthorized",
				Status:    http.StatusUnauthorized,
				Detail:    "missing or invalid x-internal-token header",
				Instance:  c.Request.URL.Path,
				Code:      "UNAUTHORIZED",
				TraceID:   c.Writer.Header().Get("X-Trace-ID"),
				RequestID: c.Writer.Header().Get("X-Request-ID"),
			})
			return
		}
		c.Next()
	}
}
