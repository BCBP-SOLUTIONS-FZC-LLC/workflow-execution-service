package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// redirecting answers every request with a 307 to a second server and counts
// the requests that reach it. The clients forward the system caller identity as
// headers, which a followed redirect would carry to whatever the Location names.
func redirecting(t *testing.T) (url string, targetHits *atomic.Int32) {
	t.Helper()
	targetHits = &atomic.Int32{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(front.Close)
	return front.URL, targetHits
}

func requireRedirectRefused(t *testing.T, err error, targetHits *atomic.Int32) {
	t.Helper()
	if err == nil {
		t.Fatal("a 307 is a failed call, got no error")
	}
	if n := targetHits.Load(); n != 0 {
		t.Fatalf("the redirect target was called %d times, want 0", n)
	}
}

func TestEntitlementsClient_DoesNotFollowRedirects(t *testing.T) {
	url, hits := redirecting(t)
	_, err := NewEntitlementsClient(url, time.Second).FeatureFlags(context.Background(), uuid.New())
	requireRedirectRefused(t, err, hits)
}

func TestDefinitionClient_DoesNotFollowRedirects(t *testing.T) {
	url, hits := redirecting(t)
	_, err := NewDefinitionClient(url, time.Second).GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New())
	requireRedirectRefused(t, err, hits)
}

func TestEligibilityClient_DoesNotFollowRedirects(t *testing.T) {
	url, hits := redirecting(t)
	_, err := NewEligibilityClient(url, time.Second).CheckEligibility(context.Background(), uuid.New(), uuid.New(), uuid.New(), "reviewer")
	requireRedirectRefused(t, err, hits)
}

func TestAutomationPrincipalClient_DoesNotFollowRedirects(t *testing.T) {
	url, hits := redirecting(t)
	_, err := NewAutomationPrincipalClient(url, time.Second, time.Minute).AutomationPrincipal(context.Background(), uuid.New())
	requireRedirectRefused(t, err, hits)
}
