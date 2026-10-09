//go:build integration

package telemetry_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgmetrics"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestDatabaseMetricsAreEmitted(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test",
	})
	_, err := pgmetrics.InitWithIdentity(pgmetrics.IdentityFromLabels(gincommon.MetricsConstLabels()), gincommon.MetricsRegisterer())
	require.NoError(t, err)

	pool := fixtures.NewTestPool(t)
	ctx := context.Background()
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, execErr := conn.Exec(ctx, "SELECT 1")
		return execErr
	}))
	require.NoError(t, pool.WithTx(ctx, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		_, execErr := tx.Exec(ctx, "SELECT 1")
		return execErr
	}))

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range gathered {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"platform_db_queries_total",
		"platform_db_query_duration_seconds",
		"platform_db_pool_acquires_total",
		"platform_db_pool_connections",
		"platform_db_pool_max_connections",
	} {
		assert.True(t, names[want], "%s must be exported by the pool", want)
	}
}
