package handler

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

// idempotencyClaim is the placeholder value SetNX stakes atomically before
// the wrapped handler runs, so two requests racing on the same
// Idempotency-Key can't both win the check-then-act window a plain Get/Set
// leaves open. A raw string, not JSON, so it's trivially distinguishable
// from a real cachedResp without risking a false "corrupted entry" read.
const idempotencyClaim = "CLAIMED"

// WithIdempotency wraps a Gin handler with Idempotency-Key support (LLD
// §5.9), mirroring definition_service's own implementation.
//
// On the first call with a given Idempotency-Key, SetNX atomically claims
// the key before the handler runs; on success the handler executes and its
// response (status + body) plus a SHA-256 hash of the request body replace
// the claim in cache under the given TTL.
//
// On subsequent requests with the same key:
//   - If the claim hasn't resolved into a real response yet — a concurrent
//     request is still executing, or it crashed before storing one — the
//     request is rejected with IDEMPOTENCY_KEY_REPLAY (409) rather than
//     also executing the handler.
//   - If the request body hash matches the stored hash, the cached response
//     is returned without re-executing the handler (standard idempotent replay).
//   - If the hash differs, the request is rejected with IDEMPOTENCY_KEY_REPLAY
//     (409) so callers know they reused a key with a different payload.
//
// Only 2xx responses are cached; a non-2xx response releases the claim
// (deletes the key) instead, so callers can retry immediately after fixing
// the request rather than waiting out the full TTL.
//
// If cache is nil (dev/test, or before T2.1 wires a real Valkey client) or
// the header is absent, the handler runs as-is with no idempotency
// enforcement. A SetNX/cache failure is fail-open (LLD §5.9: Valkey being
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

		bodyBytes, incomingHash, err := drainBody(c)
		if err != nil {
			bindErrResponse(c, err)
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

		claimed, err := cache.SetNX(c.Request.Context(), cacheKey, idempotencyClaim, ttl)
		if err != nil {
			idempotencyLogWarn(log, "idempotency: cache claim failed, proceeding without idempotency enforcement", map[string]any{
				"cache_key": cacheKey, "error": err.Error(),
			})
			h(c)
			return
		}
		if !claimed {
			if !replayIfCached(c, cache, cacheKey, incomingHash, log) {
				errResponse(c, port.ErrIdempotencyKeyReplay)
			}
			return
		}

		rec := &bodyRecorder{ResponseWriter: c.Writer, buf: &bytes.Buffer{}}
		c.Writer = rec
		h(c)
		storeResult(c, cache, cacheKey, incomingHash, ttl, rec, log)
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

func drainBody(c *gin.Context) (body []byte, hash string, err error) {
	b, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes))
	if err != nil {
		return nil, "", fmt.Errorf("drain request body: %w", err)
	}
	return b, hashBody(b), nil
}

// replayIfCached reports whether it fully handled the response (a genuine
// replay, a hash-mismatch rejection, or an unresolved-claim rejection).
// false means the cache entry couldn't be read or understood at all — the
// caller (WithIdempotency) still must not run the handler, since SetNX
// already reported this key as claimed by someone else.
func replayIfCached(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, log port.Logger) bool {
	raw, err := cache.Get(c.Request.Context(), cacheKey)
	if err != nil {
		idempotencyLogWarn(log, "idempotency: cache get failed after a claimed key, rejecting replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
		return false
	}
	if raw == "" || raw == idempotencyClaim {
		// Either the claim already expired out from under us, or another
		// request is still executing the handler — either way, not our
		// response to serve.
		return false
	}
	var cr cachedResp
	if err := json.Unmarshal([]byte(raw), &cr); err != nil {
		idempotencyLogWarn(log, "idempotency: corrupted cache entry for a claimed key, rejecting replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
		return false
	}
	if cr.BodyHash != "" && cr.BodyHash != incomingHash {
		errResponse(c, port.ErrIdempotencyKeyReplay)
		return true
	}
	c.Data(cr.Status, "application/json", cr.Body)
	return true
}

// storeResult replaces the SetNX claim with the real response on success, or
// releases it on failure so a caller with a fixed request can retry
// immediately with the same key rather than waiting out the full TTL.
func storeResult(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, ttl time.Duration, rec *bodyRecorder, log port.Logger) {
	status := rec.Status()
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		if err := cache.Del(c.Request.Context(), cacheKey); err != nil {
			idempotencyLogWarn(log, "idempotency: failed to release claim after a non-2xx response", map[string]any{
				"cache_key": cacheKey, "error": err.Error(),
			})
		}
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
