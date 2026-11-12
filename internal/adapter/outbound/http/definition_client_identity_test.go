package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The Definition Service admits an internal call by the system caller identity
// headers; the retired shared token is never sent.
func TestDefinitionClient_SendsTheSystemCallerIdentity(t *testing.T) {
	tenant := uuid.New()
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewDefinitionClient(srv.URL, time.Second)
	_, _ = c.GetCompiledWorkflow(context.Background(), tenant, uuid.New())

	if v := got.Get("x-user-id"); v != "iam-system" {
		t.Errorf("x-user-id = %q, want iam-system", v)
	}
	if v := got.Get("x-tenant-roles"); v != "iam-system" {
		t.Errorf("x-tenant-roles = %q, want iam-system", v)
	}
	if v := got.Get("x-tenant-id"); v != tenant.String() {
		t.Errorf("x-tenant-id = %q, want %s", v, tenant)
	}
	if _, sent := got["X-Internal-Token"]; sent {
		t.Error("x-internal-token was sent; the shared token is removed")
	}
}
