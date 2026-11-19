package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w25aCache is a thread-safe in-memory cache that records the key, value and
// lifetime of every claim and store, and every read and delete.
type w25aCache struct {
	mu   sync.Mutex
	data map[string]string

	getErr error

	setNXKeys []string
	setNXVals []string
	setNXTTLs []time.Duration
	setKeys   []string
	setVals   []string
	setTTLs   []time.Duration
	getKeys   []string
	delKeys   []string
}

func w25aNewCache() *w25aCache { return &w25aCache{data: map[string]string{}} }

func (c *w25aCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getKeys = append(c.getKeys, key)
	if c.getErr != nil {
		return "", c.getErr
	}
	return c.data[key], nil
}

func (c *w25aCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setKeys = append(c.setKeys, key)
	c.setVals = append(c.setVals, value)
	c.setTTLs = append(c.setTTLs, ttl)
	c.data[key] = value
	return nil
}

func (c *w25aCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delKeys = append(c.delKeys, keys...)
	for _, k := range keys {
		delete(c.data, k)
	}
	return nil
}

func (c *w25aCache) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setNXKeys = append(c.setNXKeys, key)
	c.setNXVals = append(c.setNXVals, value)
	c.setNXTTLs = append(c.setNXTTLs, ttl)
	if _, held := c.data[key]; held {
		return false, nil
	}
	c.data[key] = value
	return true, nil
}

func (c *w25aCache) Ping(context.Context) error { return nil }

var _ port.CacheStore = (*w25aCache)(nil)

// w25aWarning is one recorded warn log line.
type w25aWarning struct {
	msg    string
	fields map[string]any
}

// w25aLog records every warning; its other levels are ignored.
type w25aLog struct {
	mu    sync.Mutex
	warns []w25aWarning
}

func (l *w25aLog) Debug(string, map[string]any) {}
func (l *w25aLog) Info(string, map[string]any)  {}
func (l *w25aLog) Error(string, map[string]any) {}
func (l *w25aLog) Warn(msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, w25aWarning{msg: msg, fields: fields})
}

var _ port.Logger = (*w25aLog)(nil)

// w25aProblem is the full problem document of an error answer.
type w25aProblem struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail"`
	Instance      string `json:"instance"`
	Code          string `json:"code"`
	InvalidParams []struct {
		Name   string `json:"name"`
		Reason string `json:"reason"`
		Code   string `json:"code"`
	} `json:"invalid_params"`
}

// w25aDecodeProblem fails unless w is a problem document with the given status and returns it.
func w25aDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w25aProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var p w25aProblem
	decodeJSON(t, w.Body, &p)
	assert.Equal(t, status, p.Status)
	return p
}

// w25aRaw builds a request with gateway identity headers and a raw body.
func w25aRaw(method, target, contentType, body string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	return r
}

// w25aServe runs one request through the router.
func w25aServe(router *gin.Engine, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func w25aInstancePath(id uuid.UUID, tail string) string {
	return "/api/v1/instances/" + id.String() + tail
}
