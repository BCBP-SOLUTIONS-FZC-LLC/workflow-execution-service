//go:build integration

package observability_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRegisterPoolStats_Scrapable(t *testing.T) {
	pool := fixtures.NewTestPool(t)

	observability.RegisterPoolStats(pool, "test-pool")

	srv := httptest.NewServer(promhttp.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL) //nolint:noctx,gosec // test-only, fixed httptest URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "pgcommon_pool_total_conns")
}
