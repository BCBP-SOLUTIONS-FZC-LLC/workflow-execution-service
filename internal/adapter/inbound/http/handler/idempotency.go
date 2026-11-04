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
	return r.ResponseWriter.Write(b) //nolint:wrapcheck // a recorder passes the wrapped writer's result through unchanged
}

func hashBody(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

const idempotencyClaim = "CLAIMED"

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
	b, _ := json.Marshal(entry)
	if err := cache.Set(c.Request.Context(), cacheKey, string(b), ttl); err != nil {
		idempotencyLogWarn(log, "idempotency: cache set failed, response not cached for replay", map[string]any{
			"cache_key": cacheKey, "error": err.Error(),
		})
	}
}
