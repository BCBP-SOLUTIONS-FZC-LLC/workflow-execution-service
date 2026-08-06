//go:build integration

// Package postgres_test is the dedicated negative-path RLS suite required by
// the Tier-0 bootstrap: proves the centralized app_tenant_id() function
// (db/migrations/000001_create_schema.up.sql) actually blocks cross-tenant
// reads, and that RLS is FORCE-enabled on every policy-bearing table — not
// just that the migrations applied without error.
package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// appRolePassword is a throwaway credential for the per-test Postgres
// container's dedicated non-superuser role — not a real secret.
const appRolePassword = "apppassword123"

// withGUC injects a tenant-scoped GUCSet into ctx so pgcommon.GUCSetFromContext
// (wired as the app-role pool's GUCProvider) applies it before each query.
func withGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, domain.GUCSet{TenantID: tenantID.String()})
}

// hostAndDB extracts the "host:port/dbname?params" suffix from a DSN so a new
// DSN can be built for a different role against the same running container.
func hostAndDB(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	return strings.TrimPrefix(dsn, u.Scheme+"://"+u.User.String()+"@")
}

// newAppRolePool creates a dedicated, non-superuser Postgres role (no
// BYPASSRLS) against the same container as superDSN, grants it just enough
// privilege to exercise the tenant-scoped tables, and returns a pool connected
// as that role with RLS fully enforced through it.
func newAppRolePool(t *testing.T, superPool *pgcommon.Pool, superDSN string) *pgcommon.Pool {
	t.Helper()
	ctx := context.Background()

	stmts := []string{
		"DROP ROLE IF EXISTS execution_app",
		fmt.Sprintf("CREATE ROLE execution_app LOGIN PASSWORD '%s'", appRolePassword),
		"GRANT CONNECT ON DATABASE testdb TO execution_app",
		"GRANT USAGE ON SCHEMA public TO execution_app",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO execution_app",
		"GRANT EXECUTE ON FUNCTION app_tenant_id() TO execution_app",
	}
	for _, stmt := range stmts {
		stmt := stmt
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, stmt)
			return err
		}))
	}

	appDSN := fmt.Sprintf("postgres://execution_app:%s@%s", appRolePassword, hostAndDB(superDSN))
	appPool, err := pgcommon.NewPool(ctx, pgcommon.Config{
		DSN:         appDSN,
		GUCProvider: pgcommon.GUCSetFromContext,
	})
	require.NoError(t, err)
	t.Cleanup(appPool.Close)

	return appPool
}

func seedWorkflowInstance(t *testing.T, superPool *pgcommon.Pool, ctx context.Context, id, tenantID uuid.UUID) {
	t.Helper()
	err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `
			INSERT INTO workflow_instance (
				id, tenant_id, workflow_id, workflow_version_id, business_key,
				temporal_workflow_id, status, current_node_keys, task_queue, started_by_user_id
			) VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING', '{}', 'execution-default', $7)`,
			id, tenantID, uuid.New(), uuid.New(), "rls-test-"+id.String(),
			"wf-"+id.String(), uuid.New(),
		)
		return err
	})
	require.NoError(t, err)
}

func countWorkflowInstance(t *testing.T, pool *pgcommon.Pool, ctx context.Context, id uuid.UUID) int {
	t.Helper()
	var count int
	err := pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM workflow_instance WHERE id = $1", id).Scan(&count)
	})
	require.NoError(t, err)
	return count
}

func rlsFlags(t *testing.T, pool *pgcommon.Pool, ctx context.Context, table string) (enabled, forced bool) {
	t.Helper()
	err := pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass",
			table,
		).Scan(&enabled, &forced)
	})
	require.NoError(t, err)
	return enabled, forced
}

func TestRLSPolicyEnforcement(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)

	tenantA := uuid.New()
	tenantB := uuid.New()
	instanceID := uuid.New()

	ctx := context.Background()
	seedWorkflowInstance(t, superPool, ctx, instanceID, tenantA)

	t.Run("same tenant can read the instance", func(t *testing.T) {
		found := countWorkflowInstance(t, appPool, withGUC(ctx, tenantA), instanceID)
		assert.Equal(t, 1, found)
	})

	t.Run("different tenant sees zero rows (RLS fail-closed)", func(t *testing.T) {
		found := countWorkflowInstance(t, appPool, withGUC(ctx, tenantB), instanceID)
		assert.Equal(t, 0, found)
	})

	t.Run("missing GUC sees zero rows (fail-closed)", func(t *testing.T) {
		found := countWorkflowInstance(t, appPool, ctx, instanceID)
		assert.Equal(t, 0, found)
	})
}

func TestRLSForceEnabledOnTenantTables(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()

	policyBearing := []string{
		"workflow_instance", "workflow_task", "workflow_task_assignment",
		"workflow_data_keys", "assignee_overrides",
	}
	for _, table := range policyBearing {
		enabled, forced := rlsFlags(t, superPool, ctx, table)
		assert.Truef(t, enabled, "%s: relrowsecurity should be true", table)
		assert.Truef(t, forced, "%s: relforcerowsecurity should be true", table)
	}

	exceptions := []string{"active_task_queues", "processed_event", "event_recency_guard"}
	for _, table := range exceptions {
		enabled, _ := rlsFlags(t, superPool, ctx, table)
		assert.Falsef(t, enabled, "%s: relrowsecurity should be false (named RLS exception)", table)
	}
}

func TestAppRoleLacksBypassRLS(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	_ = newAppRolePool(t, superPool, superDSN) // ensures the role exists
	ctx := context.Background()

	var bypassRLS, isSuper bool
	err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT rolbypassrls, rolsuper FROM pg_roles WHERE rolname = 'execution_app'",
		).Scan(&bypassRLS, &isSuper)
	})
	require.NoError(t, err)
	assert.False(t, bypassRLS, "execution_app must not have BYPASSRLS")
	assert.False(t, isSuper, "execution_app must not be a superuser")
}

// outboxRelayRolePassword is a throwaway credential for the per-test outbox
// relay role — not a real secret.
const outboxRelayRolePassword = "relaypassword123"

// newOutboxRelayRolePool creates the dedicated BYPASSRLS role the outbox
// relay connects as (LLD §9.2/§9.7).
func newOutboxRelayRolePool(t *testing.T, superPool *pgcommon.Pool, superDSN string) *pgcommon.Pool {
	t.Helper()
	ctx := context.Background()

	stmts := []string{
		"DROP ROLE IF EXISTS execution_outbox_relay",
		fmt.Sprintf("CREATE ROLE execution_outbox_relay LOGIN PASSWORD '%s' BYPASSRLS", outboxRelayRolePassword),
		"GRANT CONNECT ON DATABASE testdb TO execution_outbox_relay",
		"GRANT USAGE ON SCHEMA public TO execution_outbox_relay",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO execution_outbox_relay",
	}
	for _, stmt := range stmts {
		stmt := stmt
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, stmt)
			return err
		}))
	}

	relayDSN := fmt.Sprintf("postgres://execution_outbox_relay:%s@%s", outboxRelayRolePassword, hostAndDB(superDSN))
	relayPool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: relayDSN})
	require.NoError(t, err)
	t.Cleanup(relayPool.Close)

	return relayPool
}

// TestOutboxRelayRoleHasBypassRLS is the positive control to
// TestAppRoleLacksBypassRLS — without it, that test passing could just mean
// rolbypassrls always reads false.
func TestOutboxRelayRoleHasBypassRLS(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	_ = newOutboxRelayRolePool(t, superPool, superDSN) // ensures the role exists
	ctx := context.Background()

	var bypassRLS, isSuper bool
	err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT rolbypassrls, rolsuper FROM pg_roles WHERE rolname = 'execution_outbox_relay'",
		).Scan(&bypassRLS, &isSuper)
	})
	require.NoError(t, err)
	assert.True(t, bypassRLS, "execution_outbox_relay must have BYPASSRLS")
	assert.False(t, isSuper, "execution_outbox_relay must not be a superuser")
}

// TestRLSPoliciesUseCheckTenantFunction is a definition-level check distinct
// from TestRLSPolicyEnforcement's behavioral one, which would pass unchanged
// whether 000006's policy swap actually landed or not.
func TestRLSPoliciesUseCheckTenantFunction(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()

	swapped := []string{
		"workflow_instance", "workflow_task", "workflow_task_assignment",
		"workflow_data_keys", "assignee_overrides", "outbox_events", "outbox_dead_letters",
	}
	for _, table := range swapped {
		t.Run(table, func(t *testing.T) {
			var qual string
			err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				return conn.QueryRow(ctx,
					"SELECT qual FROM pg_policies WHERE schemaname = 'public' AND tablename = $1 AND policyname = 'tenant_isolation_policy'",
					table,
				).Scan(&qual)
			})
			require.NoError(t, err)
			assert.Contains(t, qual, "rls_check_tenant", "%s: policy should use rls_check_tenant after 000006", table)
		})
	}
}

// TestRLSViolationAuditLogging is a best-effort test for log_rls_violation's
// 1%-sampled INSERT path — a large attempt count makes a hit a near-certainty
// rather than asserting exactly one per attempt.
func TestRLSViolationAuditLogging(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()

	tenantA := uuid.New()
	tenantB := uuid.New()
	instanceID := uuid.New()
	seedWorkflowInstance(t, superPool, ctx, instanceID, tenantA)

	const attempts = 500
	for i := 0; i < attempts; i++ {
		_ = countWorkflowInstance(t, appPool, withGUC(ctx, tenantB), instanceID)
	}

	var count int
	err := superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT count(*) FROM rls_violation_log WHERE violation_type = 'cross_tenant_access' AND table_name = 'workflow_instance'",
		).Scan(&count)
	})
	require.NoError(t, err)
	assert.Greaterf(t, count, 0,
		"expected at least one sampled rls_violation_log row across %d cross-tenant attempts (~1%% sampling)", attempts)
}
