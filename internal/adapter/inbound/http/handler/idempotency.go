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
// enforcement — unlike definition_service's version, this one does not take
// a logger: execution_service has no port.Logger abstraction yet, and the
// one rare failure mode this would log (a request-body read error) simply
// bypasses idempotency protection instead, matching the wrapper's own
// existing "run the handler anyway" fallback behavior for that case.
func WithIdempotency(cache port.CacheStore, ttl time.Duration, h gin.HandlerFunc) gin.HandlerFunc {
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

		// Key is scoped to method + path (concrete resource IDs, if any, are
		// part of the path already) + the client-supplied key. tenant_id
		// isn't read here — these internal routes have no gateway identity to
		// scope by, and body-hash comparison already prevents cross-tenant
		// key reuse from replaying the wrong tenant's cached response.
		cacheKey := "idem:" + c.Request.Method + ":" + c.Request.URL.Path + ":" + key
		if replayed := replayIfCached(c, cache, cacheKey, incomingHash); replayed {
			return
		}

		rec := &bodyRecorder{ResponseWriter: c.Writer, buf: &bytes.Buffer{}}
		c.Writer = rec
		h(c)
		storeIfSuccess(c, cache, cacheKey, incomingHash, ttl, rec)
	}
}

func drainBody(c *gin.Context) (body []byte, hash string, ok bool) {
	b, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, "", false
	}
	return b, hashBody(b), true
}

func replayIfCached(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string) bool {
	raw, err := cache.Get(c.Request.Context(), cacheKey)
	if err != nil || raw == "" {
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

func storeIfSuccess(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, ttl time.Duration, rec *bodyRecorder) {
	status := rec.Status()
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return
	}
	entry := cachedResp{Status: status, Body: rec.buf.Bytes(), BodyHash: incomingHash}
	b, _ := json.Marshal(entry) // cachedResp contains only JSON-safe types; Marshal never errors
	_ = cache.Set(c.Request.Context(), cacheKey, string(b), ttl)
}
