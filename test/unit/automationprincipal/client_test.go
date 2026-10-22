package automationprincipal_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	tenantA     = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	tenantB     = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	principalA  = uuid.MustParse("2b1f0000-0000-4000-8000-00000000000a")
	principalB  = uuid.MustParse("2b1f0000-0000-4000-8000-00000000000b")
	iamSystemID = "00000000-0000-0000-0000-0000000000a1"
)

func tokenService(t *testing.T, hits *atomic.Int32, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/api/v1/internal/tenants/" + tenantA.String() + "/service-accounts/platform-automation":
			assert.Equal(t, iamSystemID, r.Header.Get("x-user-id"))
			assert.Equal(t, tenantA.String(), r.Header.Get("x-tenant-id"))
			if status != http.StatusOK {
				w.WriteHeader(status)
				return
			}
			_, _ = w.Write([]byte(`{"principal_id":"` + principalA.String() + `","principal_sub":"7f3c","status":"active"}`))
		case "/api/v1/internal/tenants/" + tenantB.String() + "/service-accounts/platform-automation":
			_, _ = w.Write([]byte(`{"principal_id":"` + principalB.String() + `","principal_sub":"9b2d","status":"active"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAutomationPrincipal_ReturnsThePrincipalID(t *testing.T) {
	var hits atomic.Int32
	srv := tokenService(t, &hits, http.StatusOK)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

	got, err := c.AutomationPrincipal(context.Background(), tenantA)

	require.NoError(t, err)
	assert.Equal(t, port.AutomationPrincipal{ID: principalA, Subject: "7f3c"}, got, "the stable principal_id is the recorded actor; the sub is what a connector call names")
}

func TestAutomationPrincipal_AnswerIsCachedPerTenant(t *testing.T) {
	var hits atomic.Int32
	srv := tokenService(t, &hits, http.StatusOK)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

	for range 3 {
		got, err := c.AutomationPrincipal(context.Background(), tenantA)
		require.NoError(t, err)
		assert.Equal(t, principalA, got.ID)
	}
	other, err := c.AutomationPrincipal(context.Background(), tenantB)
	require.NoError(t, err)
	assert.Equal(t, principalB, other.ID, "another tenant is not served tenant A's principal")
	assert.EqualValues(t, 2, hits.Load(), "one read per tenant inside the cache lifetime")
}

func TestAutomationPrincipal_ReadAgainAfterTheCacheLifetime(t *testing.T) {
	var hits atomic.Int32
	srv := tokenService(t, &hits, http.StatusOK)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Millisecond)

	_, err := c.AutomationPrincipal(context.Background(), tenantA)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	_, err = c.AutomationPrincipal(context.Background(), tenantA)
	require.NoError(t, err)

	assert.EqualValues(t, 2, hits.Load())
}

func TestAutomationPrincipal_NoPrincipalIsNotCached(t *testing.T) {
	var hits atomic.Int32
	srv := tokenService(t, &hits, http.StatusNotFound)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

	for range 2 {
		_, err := c.AutomationPrincipal(context.Background(), tenantA)
		require.ErrorIs(t, err, port.ErrNoAutomationPrincipal)
	}
	assert.EqualValues(t, 2, hits.Load(), "a tenant minted later must be found without waiting out a cached miss")
}

func TestAutomationPrincipal_ServerErrorIsUpstreamUnavailable(t *testing.T) {
	var hits atomic.Int32
	srv := tokenService(t, &hits, http.StatusInternalServerError)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

	_, err := c.AutomationPrincipal(context.Background(), tenantA)

	require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
	require.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
}

func TestAutomationPrincipal_UnconfiguredHasNoPrincipal(t *testing.T) {
	c := httpadapter.NewAutomationPrincipalClient("", time.Second, time.Minute)

	_, err := c.AutomationPrincipal(context.Background(), tenantA)

	require.ErrorIs(t, err, port.ErrNoAutomationPrincipal)
}

func TestAutomationPrincipal_RevokedPrincipalIsNoPrincipal(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"principal_id":"` + principalA.String() + `","status":"revoked"}`))
	}))
	t.Cleanup(srv.Close)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

	for range 2 {
		_, err := c.AutomationPrincipal(context.Background(), tenantA)
		require.ErrorIs(t, err, port.ErrNoAutomationPrincipal, "a revoked principal can no longer act")
	}
	assert.EqualValues(t, 2, hits.Load(), "a revoked answer is not cached")
}

func TestAutomationPrincipal_UnreachableTokenServiceIsUpstreamUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	c := httpadapter.NewAutomationPrincipalClient(url, time.Second, time.Minute)

	_, err := c.AutomationPrincipal(context.Background(), tenantA)

	require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
}

func TestAutomationPrincipal_UnreadableAnswerIsUpstreamUnavailable(t *testing.T) {
	for name, body := range map[string]string{
		"not json":         `<html>`,
		"no principal_id":  `{"status":"active"}`,
		"nil principal_id": `{"principal_id":"00000000-0000-0000-0000-000000000000","status":"active"}`,
		"malformed uuid":   `{"principal_id":"nope","status":"active"}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
			t.Cleanup(srv.Close)
			c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

			_, err := c.AutomationPrincipal(context.Background(), tenantA)

			require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
		})
	}
}
