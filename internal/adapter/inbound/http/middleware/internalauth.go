package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

const InternalTokenHeader = "x-internal-token"

// RequireInternalToken guards the internal route group (e.g. the
// /internal/workflows/* family, LLD §5.7). When token is empty the check is
// disabled (local/dev) and requests pass through; NetworkPolicy / service
// mesh remains the primary control. When token is set, a request missing or
// mismatching the x-internal-token header is rejected with 401. The
// comparison is constant-time to avoid a timing side-channel on the shared
// secret.
func RequireInternalToken(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if token == "" {
			c.Next()
			return
		}
		provided := c.GetHeader(InternalTokenHeader)
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}
