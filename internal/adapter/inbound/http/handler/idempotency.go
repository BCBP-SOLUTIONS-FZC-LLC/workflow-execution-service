package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
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
	return r.ResponseWriter.Write(b) //nolint:wrapcheck // a recorder passes the wrapped writer's result through unchanged
}

func hashBody(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

const idempotencyClaim = "CLAIMED"

// idempotencyClaimTTL bounds how long an unanswered claim blocks retries: twice the router's
// request timeout, so a pod that dies mid-request frees its key soon, while a live request
// keeps it. Storing the answer extends the key to the full lifetime.
const idempotencyClaimTTL = time.Minute

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

		cacheKey := "idem:" + idempotencyScopePrefix(c, bodyBytes) + c.Request.Method + ":" + c.Request.URL.Path + ":" + key

		claimed, err := cache.SetNX(c.Request.Context(), cacheKey, idempotencyClaim, min(ttl, idempotencyClaimTTL))
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
		stored := false
		defer func() {
			if !stored {
				releaseClaim(c, cache, cacheKey, log)
			}
		}()
		h(c)
		stored = storeResult(c, cache, cacheKey, incomingHash, ttl, rec, log)
	}
}

func idempotencyScopePrefix(c *gin.Context, body []byte) string {
	if rc, ok := gincommon.RequestContext(c); ok && rc.TenantID != "" {
		return rc.TenantID + ":" + rc.UserID + ":"
	}
	var scoped struct {
		TenantID string `json:"tenant_id"`
	}
	if json.Unmarshal(body, &scoped) != nil || scoped.TenantID == "" {
		return ""
	}
	return scoped.TenantID + ":" + domain.SystemCaller + ":"
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

func replayIfCached(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, log port.Logger) bool {
	raw, err := cache.Get(c.Request.Context(), cacheKey)
	if err != nil {
		idempotencyLogWarn(log, "idempotency: cache get failed after a claimed key, rejecting replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
		return false
	}
	if raw == "" || raw == idempotencyClaim {
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

func releaseClaim(c *gin.Context, cache port.CacheStore, cacheKey string, log port.Logger) {
	if err := cache.Del(context.WithoutCancel(c.Request.Context()), cacheKey); err != nil {
		idempotencyLogWarn(log, "idempotency: failed to release the claim of an attempt that stored no answer", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
	}
}

func storeResult(c *gin.Context, cache port.CacheStore, cacheKey, incomingHash string, ttl time.Duration, rec *bodyRecorder, log port.Logger) bool {
	status := rec.Status()
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return false
	}
	entry := cachedResp{Status: status, Body: rec.buf.Bytes(), BodyHash: incomingHash}
	b, _ := json.Marshal(entry)
	if err := cache.Set(c.Request.Context(), cacheKey, string(b), ttl); err != nil {
		idempotencyLogWarn(log, "idempotency: cache set failed, response not cached for replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
		return false
	}
	return true
}
