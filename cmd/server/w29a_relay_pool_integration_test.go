//go:build integration

package main

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const w29aRolePassword = "w29a-role-password-1"

// w29aRoleDSN creates a login role with the given attribute on the test
// database, grants it the table rights the service needs, and returns a DSN
// that connects as that role.
func w29aRoleDSN(t *testing.T, superDSN, role, attribute string) string {
	t.Helper()
	statements := []string{
		"DROP ROLE IF EXISTS " + role,
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s' %s", role, w29aRolePassword, attribute),
		fmt.Sprintf("GRANT CONNECT ON DATABASE testdb TO %s", role),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", role),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s", role),
		fmt.Sprintf("GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO %s", role),
		fmt.Sprintf("GRANT EXECUTE ON FUNCTION app_tenant_id() TO %s", role),
	}
	w9aExec(t, superDSN, statements...)
	u, err := url.Parse(superDSN)
	require.NoError(t, err)
	u.User = url.UserPassword(role, w29aRolePassword)
	return u.String()
}

func w29aCount(t *testing.T, pool *pgcommon.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, query, args...).Scan(&n)
	}))
	return n
}

// The relay pool connects as the role named by OUTBOX_RELAY_DATABASE_URL,
// which skips row-level security; the app pool keeps the restricted role. The
// components built on the relay pool (the outbox runner that the retention
// sweeper prunes through, and the stats sampler) therefore see every tenant's
// rows, while the app pool without a tenant sees none.
func TestNewApp_TheRelayPoolRunsAsTheBypassRoleAndTheComponentsBuiltOnItSeeEveryTenant(t *testing.T) {
	_, superDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	appDSN := w29aRoleDSN(t, superDSN, "w29a_app", "NOBYPASSRLS")
	relayDSN := w29aRoleDSN(t, superDSN, "w29a_relay", "BYPASSRLS")

	tenantA, tenantB := uuid.New(), uuid.New()
	oldOutboxID := uuid.New()
	w9aWithConn(t, superDSN, func(ctx context.Context, conn *pgxpool.Conn) error {
		for _, seed := range []struct {
			tenant uuid.UUID
			status string
		}{{tenantA, "DEGRADED"}, {tenantB, "RUNNING"}} {
			id := uuid.New()
			if _, err := conn.Exec(ctx, `
				INSERT INTO workflow_instance (
					id, tenant_id, workflow_id, workflow_version_id, subject_id, business_key,
					temporal_workflow_id, status, current_node_keys, task_queue, started_by_user_id
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '{}', 'execution-default', $9)`,
				id, seed.tenant, uuid.New(), uuid.New(), "subj-"+id.String(), "bk-"+id.String(), "wf-"+id.String(), seed.status, uuid.New(),
			); err != nil {
				return err
			}
		}
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events (id, event_type, payload, tenant_id, created_at, published_at)
			VALUES ($1, 'w29a.event', '{}', $2, now() - interval '9 years', now() - interval '8 years')`, oldOutboxID, tenantA.String())
		return err
	})
	cfg := w9aAppConfig(t, appDSN, temporalAddr, map[string]string{
		"OUTBOX_RELAY_DATABASE_URL": relayDSN,
		"OUTBOX_RETENTION":          "720h",
		"RETENTION_SWEEP_BATCH":     "1000",
	})

	a, err := newApp(cfg)

	require.NoError(t, err)
	t.Cleanup(func() { w9aCloseApp(a) })
	const countInstances = "SELECT count(*) FROM workflow_instance"
	assert.Equal(t, 0, w29aCount(t, a.appPool, countInstances), "the app pool runs under row-level security and has no tenant")
	assert.Equal(t, 2, w29aCount(t, a.relayPool, countInstances), "the relay pool sees every tenant")
	assert.Equal(t, 1, w29aCount(t, a.relayPool, "SELECT count(*) FROM outbox_events WHERE id = $1", oldOutboxID))
	assert.Equal(t, 0, w29aCount(t, a.appPool, "SELECT count(*) FROM outbox_events WHERE id = $1", oldOutboxID))

	require.NoError(t, a.statsSampler.Tick(context.Background()))
	assert.Equal(t, 1.0, obstest.Sum(t, "workflow_execution_service_instance_degraded_current", nil),
		"the sampler counted the other tenant's degraded instance through the relay pool")

	require.NoError(t, a.retentionSweeper.Tick(context.Background()))
	assert.Equal(t, 0, w29aCount(t, a.relayPool, "SELECT count(*) FROM outbox_events WHERE id = $1", oldOutboxID),
		"the sweeper pruned the published row through the relay pool")
}
