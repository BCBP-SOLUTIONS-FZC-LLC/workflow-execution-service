package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchAliases_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/internal/connector-aliases", r.URL.Path)
		assert.Equal(t, "test-token", r.Header.Get("x-internal-token"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":1,"restCall":[{"alias":"a","method":"GET","baseURL":"http://x","pathTemplate":"/y","timeout":5000000000}],"sqlQuery":[]}`))
	}))
	defer srv.Close()

	cfg, err := fetchAliases(context.Background(), srv.URL, "test-token", time.Second)
	require.NoError(t, err)
	require.Len(t, cfg.RestCall, 1)
	assert.Equal(t, "a", cfg.RestCall[0].Alias)
	assert.Equal(t, 5*time.Second, cfg.RestCall[0].Timeout)
}

func TestFetchAliases_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := fetchAliases(context.Background(), srv.URL, "test-token", time.Second)
	require.Error(t, err)
}

func TestFetchAliases_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	_, err := fetchAliases(context.Background(), srv.URL, "test-token", time.Second)
	require.Error(t, err)
}

func TestFetchAliases_ConnectionRefused(t *testing.T) {
	_, err := fetchAliases(context.Background(), "http://127.0.0.1:1", "test-token", 100*time.Millisecond)
	require.Error(t, err)
}
