//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRepos_ACallWithNoTenantScope_SeesNoRowsAndItsWritesAreRejected(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outbox := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()
	tenant := uuid.New()
	taskID := uuid.New()

	seeded := taskEventEnvelope(tenant, taskID, "WorkflowTaskSlaWarning")
	require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(txCtx context.Context) error {
		return outbox.Enqueue(txCtx, seeded)
	}))

	t.Run("a read through the repository with a bare context finds nothing", func(t *testing.T) {
		scoped, err := outbox.ExistsForTask(withGUC(ctx, tenant), "WorkflowTaskSlaWarning", taskID)
		require.NoError(t, err)
		require.True(t, scoped, "the row is there for its own tenant")

		bare, err := outbox.ExistsForTask(ctx, "WorkflowTaskSlaWarning", taskID)
		require.NoError(t, err, "an unscoped read is an empty answer, not an error")
		assert.False(t, bare)
	})

	t.Run("an outbox write inside a transaction opened with a bare context is rejected and stores nothing", func(t *testing.T) {
		rejected := taskEventEnvelope(tenant, uuid.New(), "WorkflowTaskSlaWarning")
		err := transactor.RunInTx(ctx, func(txCtx context.Context) error {
			return outbox.Enqueue(txCtx, rejected)
		})
		require.Error(t, err)
		assert.True(t, pgcommon.IsPgError(err), "the database refused the row: %v", err)

		var stored int
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE id = $1`, uuid.MustParse(rejected.ID)).Scan(&stored)
		}))
		assert.Zero(t, stored)

		accepted := taskEventEnvelope(tenant, uuid.New(), "WorkflowTaskSlaWarning")
		require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(txCtx context.Context) error {
			return outbox.Enqueue(txCtx, accepted)
		}), "the same write with the tenant scope set goes through")
	})

	t.Run("a row insert on a tenant table with a bare context is rejected and stores nothing", func(t *testing.T) {
		instanceID := uuid.New()
		insert := func(ctx context.Context) error {
			return appPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				_, err := conn.Exec(ctx, `
					INSERT INTO workflow_instance (
						id, tenant_id, workflow_id, workflow_version_id, subject_id, business_key,
						temporal_workflow_id, status, current_node_keys, task_queue, started_by_user_id
					) VALUES ($1, $2, $3, $4, 'subj', 'bk', $5, 'RUNNING', '{}', 'q', $6)`,
					instanceID, tenant, uuid.New(), uuid.New(), "wf-"+instanceID.String(), uuid.New())
				return err
			})
		}
		insertErr := insert(ctx)
		require.Error(t, insertErr)
		assert.True(t, pgcommon.IsPgError(insertErr), "the database refused the row: %v", insertErr)
		assert.Zero(t, countWorkflowInstance(t, superPool, ctx, instanceID))

		require.NoError(t, insert(withGUC(ctx, tenant)), "the same insert with the tenant scope set goes through")
		assert.Equal(t, 1, countWorkflowInstance(t, superPool, ctx, instanceID))
	})

	t.Run("bare reads leave missing_or_invalid_guc rows in the violation log", func(t *testing.T) {
		const attempts = 3000
		for i := 0; i < attempts; i++ {
			exists, err := outbox.ExistsForTask(ctx, "WorkflowTaskSlaWarning", taskID)
			require.NoError(t, err)
			require.False(t, exists)
		}
		var logged int
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx,
				`SELECT count(*) FROM rls_violation_log WHERE violation_type = 'missing_or_invalid_guc' AND table_name = 'outbox_events' AND app_tenant_id IS NULL AND row_tenant_id = $1`,
				tenant).Scan(&logged)
		}))
		assert.Greaterf(t, logged, 0, "about 1%% of %d unscoped reads are sampled into the log", attempts)
	})
}
