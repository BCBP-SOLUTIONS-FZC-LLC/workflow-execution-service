package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

const (
	invalidIdentityMessage = "missing or invalid authentication headers"

	// identityRejectedKey is the key gincommon's ContextMiddleware sets when it rejects an identity, so the
	// access log reports the headers as claimed_* instead of as the verified tenant_id and user_id.
	identityRejectedKey = "gincommon.identity_rejected"
)

// RequireUUIDIdentity runs after the gateway identity middleware and refuses
// a caller whose tenant or user id is not a UUID, with the same 401 body, the
// same warning log and the same access-log flag as that middleware gives a
// rejected identity. A nil logger skips the warning.
func RequireUUIDIdentity(logger gincommon.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		rc, ok := gincommon.RequestContext(c)
		if ok && isUUID(rc.TenantID) && isUUID(rc.UserID) {
			c.Next()
			return
		}
		rejectIdentity(c, logger, rc)
	}
}

func rejectIdentity(c *gin.Context, logger gincommon.Logger, rc *gincommon.RequestContextData) {
	traceID := c.Writer.Header().Get("X-Trace-ID")
	requestID := c.Writer.Header().Get("X-Request-ID")
	c.Set(identityRejectedKey, true)
	c.Set(gincommon.RequestContextKey, nil)
	if logger != nil {
		logger.Warn("auth: request rejected", rejectionFields(c, rc, traceID, requestID))
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gincommon.ErrorResponse{
		Error:     invalidIdentityMessage,
		Status:    http.StatusUnauthorized,
		TraceID:   traceID,
		RequestID: requestID,
	})
}

// rejectionFields mirrors gincommon's audit entry. The ids were already length- and control-character-checked
// by the gateway identity middleware, and are logged as claimed_* because they are not verified.
func rejectionFields(c *gin.Context, rc *gincommon.RequestContextData, traceID, requestID string) map[string]interface{} {
	route := c.FullPath()
	if route == "" {
		route = "unknown"
	}
	fields := map[string]interface{}{
		"reason":     "invalid identity: tenant or user id is not a UUID",
		"method":     c.Request.Method,
		"route":      route,
		"status":     http.StatusUnauthorized,
		"client_ip":  c.ClientIP(),
		"trace_id":   traceID,
		"request_id": requestID,
	}
	if rc != nil {
		fields["claimed_tenant_id"] = rc.TenantID
		fields["claimed_user_id"] = rc.UserID
	}
	return fields
}

func isUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}
