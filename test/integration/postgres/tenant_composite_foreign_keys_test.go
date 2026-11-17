//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const foreignKeyViolation = "23503"

func insertTaskRow(ctx context.Context, pool *pgcommon.Pool, id, tenantID, instanceID uuid.UUID, deferredFrom *uuid.UUID) error {
	return pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `
			INSERT INTO workflow_task (id, tenant_id, workflow_instance_id, node_key, department_id, status, assignee_mode, deferred_from_task_id)
			VALUES ($1, $2, $3, 'finance/review', $4, 'READY', 'single', $5)`,
			id, tenantID, instanceID, uuid.New(), deferredFrom)
		return err
	})
}

func requireForeignKeyViolation(t *testing.T, err error, msg string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "%s: expected a database error, got %v", msg, err)
	assert.Equal(t, foreignKeyViolation, pgErr.Code, msg)
}

func TestChildRows_CannotReferenceAnotherTenantsParent(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()
	instanceB, instanceA := uuid.New(), uuid.New()
	seedWorkflowInstance(t, superPool, ctx, instanceB, tenantB)
	seedWorkflowInstance(t, superPool, ctx, instanceA, tenantA)
	taskB, taskA := uuid.New(), uuid.New()
	require.NoError(t, insertTaskRow(ctx, superPool, taskB, tenantB, instanceB, nil))
	require.NoError(t, insertTaskRow(ctx, superPool, taskA, tenantA, instanceA, nil))

	t.Run("a task of the same tenant is accepted", func(t *testing.T) {
		require.NoError(t, insertTaskRow(ctx, superPool, uuid.New(), tenantA, instanceA, &taskA))
	})

	t.Run("a task cannot name another tenant's instance", func(t *testing.T) {
		requireForeignKeyViolation(t, insertTaskRow(ctx, superPool, uuid.New(), tenantA, instanceB, nil), "task of tenant A on tenant B's instance")
	})

	t.Run("a task cannot be deferred from another tenant's task", func(t *testing.T) {
		requireForeignKeyViolation(t, insertTaskRow(ctx, superPool, uuid.New(), tenantA, instanceA, &taskB), "task of tenant A deferred from tenant B's task")
	})

	t.Run("an assignment cannot name another tenant's task", func(t *testing.T) {
		err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, `INSERT INTO workflow_task_assignment (id, tenant_id, task_id, user_id) VALUES ($1, $2, $3, $4)`, uuid.New(), tenantA, taskB, uuid.New())
			return err
		})
		requireForeignKeyViolation(t, err, "assignment of tenant A on tenant B's task")
	})

	t.Run("an assignee override cannot name another tenant's instance", func(t *testing.T) {
		err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, `INSERT INTO assignee_overrides (id, tenant_id, workflow_instance_id, node_key, new_user_id, actor_user_id) VALUES ($1, $2, $3, 'finance/review', $4, $5)`, uuid.New(), tenantA, instanceB, uuid.New(), uuid.New())
			return err
		})
		requireForeignKeyViolation(t, err, "override of tenant A on tenant B's instance")
	})

	t.Run("an assignment and an override of the same tenant are accepted", func(t *testing.T) {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			if _, err := conn.Exec(ctx, `INSERT INTO workflow_task_assignment (id, tenant_id, task_id, user_id) VALUES ($1, $2, $3, $4)`, uuid.New(), tenantA, taskA, uuid.New()); err != nil {
				return err
			}
			_, err := conn.Exec(ctx, `INSERT INTO assignee_overrides (id, tenant_id, workflow_instance_id, node_key, new_user_id, actor_user_id) VALUES ($1, $2, $3, 'finance/review', $4, $5)`, uuid.New(), tenantA, instanceA, uuid.New(), uuid.New())
			return err
		}))
	})
}
