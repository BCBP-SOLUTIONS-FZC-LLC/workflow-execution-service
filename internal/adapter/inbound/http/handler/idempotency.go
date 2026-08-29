package handler

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

type cachedResp struct {
	Status   int    `json:"status"`
	Body     []byte `json:"body"`
	BodyHash string `json:"body_hash,omitempty"`
}

type bodyRecorder struct {
	gin.ResponseWriter
	buf *bytes.Buffer
}

func (r *bodyRecorder) Write(b []byte) (int, error) {
	r.buf.Write(b)
	return r.ResponseWriter.Write(b) //nolint:wrapcheck
}

func hashBody(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// WithIdempotency wraps a Gin handler with Idempotency-Key support (LLD
// §5.9), mirroring definition_service's own implementation.
//
// On the first call with a given Idempotency-Key the handler runs normally
// and the response (status + body) plus a SHA-256 hash of the request body
// are stored in cache under a route-scoped key with the given TTL.
//
// On subsequent requests with the same key:
//   - If the request body hash matches the stored hash, the cached response
//     is returned without re-executing the handler (standard idempotent replay).
//   - If the hash differs, the request is rejected with IDEMPOTENCY_KEY_REPLAY
//     (409) so callers know they reused a key with a different payload.
//
// Only 2xx responses are cached; error responses are never replayed so
// callers can retry after fixing the request.
//
// If cache is nil (dev/test, or before T2.1 wires a real Valkey client) or
// the header is absent, the handler runs as-is with no idempotency
// enforcement. A cache Get/Set failure is fail-open (LLD §5.9: Valkey being
// unreachable never blocks the request) and is logged at WARN via log, which
// may be nil (in which case it's silently skipped, same as every other
// logWarn call site in this package).
// Idempotent wraps fn with this Handler's own cache/TTL/log, so callers
// outside this package (router.go) can compose WithIdempotency without
// reaching into Handler's unexported fields directly.
func (h *Handler) Idempotent(fn gin.HandlerFunc) gin.HandlerFunc {
	return WithIdempotency(h.cache, h.idempotencyTTL, h.log, fn)
}

func WithIdempotency(cache port.CacheStore, ttl time.Duration, log port.Logger, h gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.GetHeader("Idempotency-Key")
		if key == "" || cache == nil {
			h(c)
			return
		}

		bodyBytes, incomingHash, ok := drainBody(c)
		if !ok {
			h(c)
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		// Key is scoped to tenant (when gateway identity is present) + method +
		// path (concrete resource IDs, if any, are part of the path already) +
		// the client-supplied key. Internal routes carry no gateway identity,
		// so idempotencyScopePrefix falls back to no tenant segment for them —
		// body-hash comparison still prevents cross-tenant key reuse there,
		// since tenant_id is always part of their request body.
		cacheKey := "idem:" + idempotencyScopePrefix(c) + c.Request.Method + ":" + c.Request.URL.Path + ":" + key
		if replayed := replayIfCached(c, cache, cacheKey, incomingHash, log); replayed {
			return
		}

		rec := &bodyRecorder{ResponseWriter: c.Writer, buf: &bytes.Buffer{}}
		c.Writer = rec
		h(c)
		storeIfSuccess(c, cache, cacheKey, incomingHash, ttl, rec, log)
	}
}

// idempotencyScopePrefix returns "<tenantID>:" when the request carries
// gateway identity, or "" for internal routes that don't (the /internal
// group router.go mounts carries no ProtectedMiddlewares).
func idempotencyScopePrefix(c *gin.Context) string {
	if rc, ok := gincommon.RequestContext(c); ok && rc.TenantID != "" {
		return rc.TenantID + ":"
	}
	return ""
}

func idempotencyLogWarn(log port.Logger, msg string, fields map[string]any) {
	if log != nil {
		log.Warn(msg, fields)
	}
}

func drainBody(c *gin.Context) (body []byte, hash string, ok bool) {
	b, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes))
	if err != nil {
		return nil, "", false
	}
	return b, hashBody(b), true
}

func replayIfCached(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, log port.Logger) bool {
	raw, err := cache.Get(c.Request.Context(), cacheKey)
	if err != nil {
		idempotencyLogWarn(log, "idempotency: cache get failed, proceeding without replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
		return false
	}
	if raw == "" {
		return false
	}
	var cr cachedResp
	if json.Unmarshal([]byte(raw), &cr) != nil {
		return false
	}
	if cr.BodyHash != "" && cr.BodyHash != incomingHash {
		errResponse(c, port.ErrIdempotencyKeyReplay)
		return true
	}
	c.Data(cr.Status, "application/json", cr.Body)
	return true
}

func storeIfSuccess(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, ttl time.Duration, rec *bodyRecorder, log port.Logger) {
	status := rec.Status()
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return
	}
	entry := cachedResp{Status: status, Body: rec.buf.Bytes(), BodyHash: incomingHash}
	b, _ := json.Marshal(entry) // cachedResp contains only JSON-safe types; Marshal never errors
	if err := cache.Set(c.Request.Context(), cacheKey, string(b), ttl); err != nil {
		idempotencyLogWarn(log, "idempotency: cache set failed, response not cached for replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
	}
}
