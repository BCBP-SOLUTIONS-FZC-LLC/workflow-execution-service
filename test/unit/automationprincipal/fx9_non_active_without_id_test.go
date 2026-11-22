package automationprincipal_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestAutomationPrincipal_ANonActivePrincipalIsNoPrincipalWhateverItsID(t *testing.T) {
	answers := map[string]string{
		"revoked, id missing": `{"status":"revoked"}`,
		"revoked, nil id":     `{"principal_id":"00000000-0000-0000-0000-000000000000","status":"revoked"}`,
		"disabled, with id":   `{"principal_id":"0190a9a8-5d1e-7c2b-9f3a-1b2c3d4e5f60","status":"disabled"}`,
	}
	for name, body := range answers {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(srv.Close)
			c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

			for range 2 {
				_, err := c.AutomationPrincipal(context.Background(), tenantA)

				require.ErrorIs(t, err, port.ErrNoAutomationPrincipal, "a principal that is not active can no longer act")
				assert.NotErrorIs(t, err, httpadapter.ErrUpstreamUnavailable, "a revoked principal is not worth retrying")
			}
			assert.EqualValues(t, 2, hits.Load(), "the answer is not cached")
		})
	}
}

func TestAutomationPrincipal_AnAnswerWithoutAStatusIsUnavailableNotRevoked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"principal_id":"0190a9a8-5d1e-7c2b-9f3a-1b2c3d4e5f60","principal_sub":"svc"}`))
	}))
	t.Cleanup(srv.Close)
	c := httpadapter.NewAutomationPrincipalClient(srv.URL, time.Second, time.Minute)

	_, err := c.AutomationPrincipal(context.Background(), tenantA)

	require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable, "a malformed answer is retried like the other missing fields")
	assert.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
}
