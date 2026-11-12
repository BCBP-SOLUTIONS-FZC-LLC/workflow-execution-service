package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

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

// RequireSystemCaller admits a request only when x-tenant-roles lists the
// system role and x-user-id is set. The headers are not a credential: the
// mesh authenticates the calling workload (AuthorizationPolicy).
func RequireSystemCaller() gin.HandlerFunc {
	return func(c *gin.Context) {
		if reason := rejectionReason(c); reason != "" {
			observability.IncCounterVec(observability.InternalAuthRejectedTotal, reason)
			c.Header("Content-Type", "application/problem+json")
			c.AbortWithStatusJSON(http.StatusUnauthorized, problemDetails{
				Type:      "https://errors.bcbp.io/execution/unauthorized",
				Title:     "Unauthorized",
				Status:    http.StatusUnauthorized,
				Detail:    "missing or invalid internal caller identity",
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

func rejectionReason(c *gin.Context) string {
	switch {
	case strings.TrimSpace(c.GetHeader("x-user-id")) == "":
		return "missing_user"
	case !hasSystemRole(c.GetHeader("x-tenant-roles")):
		return "missing_role"
	}
	return ""
}

func hasSystemRole(roles string) bool {
	for _, r := range strings.Split(roles, ",") {
		if strings.TrimSpace(r) == domain.SystemCaller {
			return true
		}
	}
	return false
}
