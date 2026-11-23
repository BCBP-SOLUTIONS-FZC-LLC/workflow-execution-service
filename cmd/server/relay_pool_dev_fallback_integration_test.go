//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRelayFallbackDSN_ARelayPoolOnTheAppRoleReadsNoOutboxOrInstanceRows(t *testing.T) {
	_, superDSN := fixtures.NewTestPoolAndDSN(t)
	appDSN := w29aRoleDSN(t, superDSN, "w29a_app_dev", "NOBYPASSRLS")
	tenantID, oldOutboxID := uuid.New(), uuid.New()
	w9aWithConn(t, superDSN, func(ctx context.Context, conn *pgxpool.Conn) error {
		if _, err := conn.Exec(ctx, `
			INSERT INTO workflow_instance (
				id, tenant_id, workflow_id, workflow_version_id, subject_id, business_key,
				temporal_workflow_id, status, current_node_keys, task_queue, started_by_user_id
			) VALUES ($1, $2, $3, $4, 'subj', 'bk', 'wf-dev', 'DEGRADED', '{}', 'execution-default', $5)`,
			uuid.New(), tenantID, uuid.New(), uuid.New(), uuid.New(),
		); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events (id, event_type, payload, tenant_id, created_at, published_at)
			VALUES ($1, 'w29a.event', '{}', $2, now() - interval '9 years', now() - interval '8 years')`, oldOutboxID, tenantID.String())
		return err
	})
	cfg := &config.Config{DatabaseURL: appDSN}
	require.Equal(t, appDSN, cfg.OutboxRelayDSN(), "with no relay URL the relay connects with the application URL")
	pool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: cfg.OutboxRelayDSN(), MaxConns: 2, MinConns: 1})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	assert.Equal(t, 0, w29aCount(t, pool, "SELECT count(*) FROM workflow_instance"), "the fallback runs under row-level security with no tenant")
	assert.Equal(t, 0, w29aCount(t, pool, "SELECT count(*) FROM outbox_events WHERE id = $1", oldOutboxID))
}
