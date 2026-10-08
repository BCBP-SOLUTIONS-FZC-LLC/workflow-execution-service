package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Execution names the actor from the tenant's automation principal, so a
// callback carries the tenant and the outcome and nothing that asserts who acted.
func TestCompletionClient_NamesNoActor(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies = append(bodies, body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, "token", 5*time.Second)

	require.NoError(t, c.Complete(context.Background(), tenant, uuid.New(), map[string]any{"k": "v"}))
	require.NoError(t, c.Fail(context.Background(), tenant, uuid.New(), "upstream_error"))

	require.Len(t, bodies, 2)
	for _, body := range bodies {
		assert.Equal(t, tenant.String(), body["tenant_id"])
		assert.NotContains(t, body, "actor_user_id")
	}
	assert.Equal(t, "upstream_error", bodies[1]["error_class"])
}
