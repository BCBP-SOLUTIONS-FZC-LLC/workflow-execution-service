package handler_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w17bCache is a cache that records every operation (and its key) it is asked to do.
type w17bCache struct {
	ops []string
}

func (c *w17bCache) Get(_ context.Context, key string) (string, error) {
	c.ops = append(c.ops, "Get "+key)
	return "", nil
}

func (c *w17bCache) Set(_ context.Context, key, _ string, _ time.Duration) error {
	c.ops = append(c.ops, "Set "+key)
	return nil
}

func (c *w17bCache) Del(_ context.Context, keys ...string) error {
	c.ops = append(c.ops, "Del "+strings.Join(keys, ","))
	return nil
}

func (c *w17bCache) SetNX(_ context.Context, key, _ string, _ time.Duration) (bool, error) {
	c.ops = append(c.ops, "SetNX "+key)
	return true, nil
}

func (c *w17bCache) Ping(context.Context) error { return nil }

var _ port.CacheStore = (*w17bCache)(nil)

// w17bCountingBody counts how often the request body was read.
type w17bCountingBody struct {
	reader io.Reader
	reads  int
}

func (b *w17bCountingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.reader.Read(p)
}

func (b *w17bCountingBody) Close() error { return nil }

// Without an Idempotency-Key header, or with no cache configured, the wrapped handler runs directly: the
// request body is untouched when it starts, nothing is claimed or stored, and each request runs the handler.
func TestWithIdempotency_NoKeyOrNoCache_RunsTheHandlerDirectly(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		cache func() *w17bCache // nil: no cache is configured
	}{
		{"key present, no cache configured", "key-1", nil},
		{"key absent, cache configured", "", func() *w17bCache { return &w17bCache{} }},
		{"key absent, no cache configured", "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cache *w17bCache
			var store port.CacheStore
			if tc.cache != nil {
				cache = tc.cache()
				store = cache
			}
			var runs int
			var readsAtStart []int
			var bodies []string
			wrapped := handler.WithIdempotency(store, time.Hour, nil, func(c *gin.Context) {
				runs++
				body := c.Request.Body.(*w17bCountingBody)
				readsAtStart = append(readsAtStart, body.reads)
				raw, err := io.ReadAll(c.Request.Body)
				require.NoError(t, err)
				bodies = append(bodies, string(raw))
				c.JSON(http.StatusCreated, gin.H{"run": runs})
			})
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.POST("/w17b", wrapped)

			var recorded []*httptest.ResponseRecorder
			for _, payload := range []string{`{"n":1}`, `{"n":1}`} {
				r := httptest.NewRequest(http.MethodPost, "/w17b", nil)
				r.Body = &w17bCountingBody{reader: strings.NewReader(payload)}
				r.Header.Set("Content-Type", "application/json")
				if tc.key != "" {
					r.Header.Set("Idempotency-Key", tc.key)
				}
				recorded = append(recorded, do(router, r))
			}

			require.Equal(t, 2, runs, "the same request sent twice runs the handler twice: nothing is replayed")
			assert.Equal(t, []int{0, 0}, readsAtStart, "the body is not read before the handler")
			assert.Equal(t, []string{`{"n":1}`, `{"n":1}`}, bodies)
			assert.Equal(t, http.StatusCreated, recorded[0].Code)
			assert.JSONEq(t, `{"run":1}`, recorded[0].Body.String())
			assert.Equal(t, http.StatusCreated, recorded[1].Code)
			assert.JSONEq(t, `{"run":2}`, recorded[1].Body.String(), "the second answer is a fresh run, not a stored one")
			if cache != nil {
				assert.Empty(t, cache.ops, "nothing is claimed, read, stored or released")
			}
		})
	}
}
