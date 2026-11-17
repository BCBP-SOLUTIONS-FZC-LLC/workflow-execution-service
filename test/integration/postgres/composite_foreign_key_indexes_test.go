//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestCompositeForeignKeys_HaveIndexesLedByTenantForTheDeleteCheck(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	cases := []struct{ table, columns string }{
		{"workflow_task_assignment", "tenant_id, task_id"},
		{"workflow_task", "tenant_id, deferred_from_task_id"},
	}
	for _, c := range cases {
		t.Run(c.table+" ("+c.columns+")", func(t *testing.T) {
			var found int
			require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				return conn.QueryRow(ctx, `
					SELECT count(*) FROM pg_indexes
					WHERE schemaname = 'public' AND tablename = $1 AND indexdef LIKE '%(' || $2 || ')%'`, c.table, c.columns).Scan(&found)
			}))
			assert.Positive(t, found, "a foreign key that references the parent by tenant and id needs a matching index on the child")
		})
	}
}
