//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// wkactTemporal answers CheckHealth with err and counts the calls; every other
// client method would panic on the nil embedded client.
type wkactTemporal struct {
	client.Client
	err   error
	calls atomic.Int32
}

func (c *wkactTemporal) CheckHealth(context.Context, *client.CheckHealthRequest) (*client.CheckHealthResponse, error) {
	c.calls.Add(1)
	return &client.CheckHealthResponse{}, c.err
}

func TestReadyz_ReportsReadyOnlyWhenBothPostgresAndTemporalAnswer(t *testing.T) {
	healthyPool, dsn := fixtures.NewTestPoolAndDSN(t)

	do := func(t *testing.T, pool *pgcommon.Pool, sdk client.Client) (int, string) {
		t.Helper()
		srv := newHealthServer("127.0.0.1:0", pool, sdk)
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec.Code, rec.Body.String()
	}

	t.Run("healthy pool and Temporal answering gives 200 with an empty body", func(t *testing.T) {
		temporal := &wkactTemporal{}

		status, body := do(t, healthyPool, temporal)

		assert.Equal(t, http.StatusOK, status)
		assert.Empty(t, body)
		assert.EqualValues(t, 1, temporal.calls.Load())
	})

	t.Run("healthy pool and Temporal failing gives 503 with an empty body", func(t *testing.T) {
		temporal := &wkactTemporal{err: context.DeadlineExceeded}

		status, body := do(t, healthyPool, temporal)

		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Empty(t, body)
		assert.EqualValues(t, 1, temporal.calls.Load())
	})

	t.Run("unhealthy pool gives 503 with an empty body and never asks Temporal", func(t *testing.T) {
		closedPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn})
		require.NoError(t, err)
		closedPool.Close()
		temporal := &wkactTemporal{}

		status, body := do(t, closedPool, temporal)

		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Empty(t, body)
		assert.EqualValues(t, 0, temporal.calls.Load(), "Temporal is not called when the pool is already unhealthy")
	})
}
