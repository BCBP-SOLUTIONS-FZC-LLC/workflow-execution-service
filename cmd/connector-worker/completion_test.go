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

func TestParseActorUserIDs(t *testing.T) {
	t.Parallel()

	tenantA, tenantB := uuid.New(), uuid.New()
	actorA, actorB := uuid.New(), uuid.New()

	tests := []struct {
		name    string
		raw     string
		wantErr bool
		want    map[uuid.UUID]uuid.UUID
	}{
		{
			name: "two tenants, whitespace tolerated",
			raw:  tenantA.String() + "=" + actorA.String() + " , " + tenantB.String() + "=" + actorB.String(),
			want: map[uuid.UUID]uuid.UUID{tenantA: actorA, tenantB: actorB},
		},
		{name: "empty", raw: "", wantErr: true},
		{name: "no separator", raw: tenantA.String(), wantErr: true},
		{name: "tenant is not a uuid", raw: "acme=" + actorA.String(), wantErr: true},
		{name: "subject is not a uuid", raw: tenantA.String() + "=automation", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseActorUserIDs(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// One worker serves every tenant, so a callback must carry the acting tenant's
// own principal rather than whichever one the process happened to start with.
func TestCompletionClient_SendsTheActingTenantsPrincipal(t *testing.T) {
	t.Parallel()

	tenantA, tenantB := uuid.New(), uuid.New()
	actorA, actorB := uuid.New(), uuid.New()

	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies = append(bodies, body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, "token",
		map[uuid.UUID]uuid.UUID{tenantA: actorA, tenantB: actorB}, 5*time.Second)

	require.NoError(t, c.Complete(context.Background(), tenantA, uuid.New(), map[string]any{}))
	require.NoError(t, c.Fail(context.Background(), tenantB, uuid.New(), "upstream_error"))

	require.Len(t, bodies, 2)
	assert.Equal(t, actorA.String(), bodies[0]["actor_user_id"])
	assert.Equal(t, actorB.String(), bodies[1]["actor_user_id"])
}

// Borrowing another tenant's principal would record an actor that does not
// exist in the acting tenant's own realm, so an unconfigured tenant fails.
func TestCompletionClient_UnconfiguredTenantFailsClosed(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("no request should be sent for a tenant with no configured principal")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, "token", map[uuid.UUID]uuid.UUID{uuid.New(): uuid.New()}, 5*time.Second)

	err := c.Complete(context.Background(), uuid.New(), uuid.New(), map[string]any{})
	assert.ErrorContains(t, err, "no automation principal configured")

	err = c.Fail(context.Background(), uuid.New(), uuid.New(), "upstream_error")
	assert.ErrorContains(t, err, "no automation principal configured")
}
