package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

const InternalTokenHeader = "x-internal-token"

// problemDetails is a minimal, local RFC-9457 body — kept independent of the
// handler package's own (unexported) writeProblem rather than importing it,
// so this middleware has no dependency on a specific adapter package.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
	Code     string `json:"code"`
}

// RequireInternalToken guards the internal route group (e.g. the
// /internal/workflows/* family, LLD §5.7). When token is empty the check is
// disabled (local/dev) and requests pass through; NetworkPolicy / service
// mesh remains the primary control. When token is set, a request missing or
// mismatching the x-internal-token header is rejected with 401 and an
// RFC-9457 problem-details body (OpenAPI's Unauthorized response schema),
// not a bare status code. The comparison is constant-time to avoid a timing
// side-channel on the shared secret.
func RequireInternalToken(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.Next()
			return
		}
		provided := c.GetHeader(InternalTokenHeader)
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, problemDetails{
				Type:     "https://api.bcbpsolutions.com/problems/unauthorized",
				Title:    "Unauthorized",
				Status:   http.StatusUnauthorized,
				Detail:   "missing or invalid x-internal-token header",
				Instance: c.Request.URL.Path,
				Code:     "UNAUTHORIZED",
			})
			return
		}
		c.Next()
	}
}
