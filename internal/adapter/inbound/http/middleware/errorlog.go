package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

// LogRequestErrors logs, once each, the causes a handler attached to the request with c.Error. The answer body
// carries only a fixed text; this log is where the wrapped cause is kept. A nil logger skips the log.
func LogRequestErrors(logger gincommon.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if logger == nil || len(c.Errors) == 0 {
			return
		}
		route := c.FullPath()
		if route == "" {
			route = "unknown"
		}
		status := c.Writer.Status()
		for _, e := range c.Errors {
			fields := map[string]interface{}{
				"method":     c.Request.Method,
				"route":      route,
				"status":     status,
				"trace_id":   c.Writer.Header().Get("X-Trace-ID"),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
				"error":      e.Err.Error(),
			}
			if status >= http.StatusInternalServerError {
				logger.Error("request failed", fields)
				continue
			}
			logger.Warn("request failed", fields)
		}
	}
}
