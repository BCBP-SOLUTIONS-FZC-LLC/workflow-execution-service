//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestNewApp_InDevWithNoRelayURLTheRelayPoolRunsAsTheAppRoleAndItsComponentsReadNothing(t *testing.T) {
	_, superDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	appDSN := w29aRoleDSN(t, superDSN, "w29a_app_dev", "NOBYPASSRLS")
	tenantID, instanceID, oldOutboxID := uuid.New(), uuid.New(), uuid.New()
	w9aWithConn(t, superDSN, func(ctx context.Context, conn *pgxpool.Conn) error {
		if _, err := conn.Exec(ctx, `
			INSERT INTO workflow_instance (
				id, tenant_id, workflow_id, workflow_version_id, subject_id, business_key,
				temporal_workflow_id, status, current_node_keys, task_queue, started_by_user_id
			) VALUES ($1, $2, $3, $4, 'subj', 'bk', 'wf-dev', 'DEGRADED', '{}', 'execution-default', $5)`,
			instanceID, tenantID, uuid.New(), uuid.New(), uuid.New(),
		); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events (id, event_type, payload, tenant_id, created_at, published_at)
			VALUES ($1, 'w29a.event', '{}', $2, now() - interval '9 years', now() - interval '8 years')`, oldOutboxID, tenantID.String())
		return err
	})
	cfg := w9aAppConfig(t, appDSN, temporalAddr, map[string]string{
		"APP_ENV":               "dev",
		"OUTBOX_RETENTION":      "720h",
		"RETENTION_SWEEP_BATCH": "1000",
	})

	a, err := newApp(cfg)

	require.NoError(t, err)
	t.Cleanup(func() { w9aCloseApp(a) })
	assert.Equal(t, 0, w29aCount(t, a.relayPool, "SELECT count(*) FROM workflow_instance"), "the fallback pool runs under row-level security with no tenant")
	assert.Equal(t, 0, w29aCount(t, a.relayPool, "SELECT count(*) FROM outbox_events WHERE id = $1", oldOutboxID))

	require.NoError(t, a.statsSampler.Tick(context.Background()))
	assert.Equal(t, 0.0, obstest.Sum(t, "workflow_execution_service_instance_degraded_current", nil), "the sampler counts nothing")
	require.NoError(t, a.retentionSweeper.Tick(context.Background()))
	stillThere := 0
	w9aWithConn(t, superDSN, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE id = $1", oldOutboxID).Scan(&stillThere)
	})
	assert.Equal(t, 1, stillThere, "the sweeper pruned nothing")
}
