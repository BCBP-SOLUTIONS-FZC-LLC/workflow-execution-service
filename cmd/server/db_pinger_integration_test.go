//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestDBPinger_Health_CopiesTheHealthOfThePool(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)
	pool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn, MaxConns: 3, MinConns: 1})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	got := dbPinger{pool: pool}.Health(context.Background())

	want := pool.Health(context.Background())
	assert.True(t, got.Healthy)
	assert.Equal(t, int32(3), got.MaxConns)
	assert.Equal(t, want.MaxConns, got.MaxConns)
	assert.Equal(t, want.Healthy, got.Healthy)
	assert.GreaterOrEqual(t, got.Utilization, 0.0)
}
