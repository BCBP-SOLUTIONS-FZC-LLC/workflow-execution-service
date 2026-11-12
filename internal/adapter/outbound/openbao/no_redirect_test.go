package openbao

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// The reader sends the vault token as a header; a followed redirect would carry
// it to whatever the Location names.
func TestReader_DoesNotFollowRedirects(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer front.Close()

	_, err := NewReader(front.URL, "tok", "secret", time.Second).Read(context.Background(), "connectors/t/x/y", "f")

	if err == nil {
		t.Fatal("a 307 is a failed read, got no error")
	}
	if hits.Load() != 0 {
		t.Fatalf("the redirect target was called %d times, want 0", hits.Load())
	}
}
