package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Execution admits the worker's callbacks by the system caller identity
// headers, naming the tenant; the retired shared token is never sent.
func TestCompletionClient_SendsTheSystemCallerIdentity(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	var seen []http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Clone())
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, 5*time.Second)
	require.NoError(t, c.Complete(context.Background(), tenant, uuid.New(), nil))
	require.NoError(t, c.Fail(context.Background(), tenant, uuid.New(), "upstream_error", nil))

	require.Len(t, seen, 2)
	for _, h := range seen {
		assert.Equal(t, "iam-system", h.Get("x-user-id"))
		assert.Equal(t, "iam-system", h.Get("x-tenant-roles"))
		assert.Equal(t, tenant.String(), h.Get("x-tenant-id"))
		assert.NotContains(t, h, "X-Internal-Token")
	}
}

// The alias registry is global, so the fetch names no tenant.
func TestFetchAliases_SendsTheSystemCallerIdentity(t *testing.T) {
	t.Parallel()

	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write([]byte(`{"version":1,"restCall":[],"sqlQuery":[]}`))
	}))
	t.Cleanup(srv.Close)

	_, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})
	require.NoError(t, err)

	assert.Equal(t, "iam-system", got.Get("x-user-id"))
	assert.Equal(t, "iam-system", got.Get("x-tenant-roles"))
	assert.Empty(t, got.Get("x-tenant-id"))
	assert.NotContains(t, got, "X-Internal-Token")
}
