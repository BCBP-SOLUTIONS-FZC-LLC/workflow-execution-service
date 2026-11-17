//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestOutboxTables_ForceRowLevelSecurity(t *testing.T) {
	superPool, _ := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()

	for _, table := range []string{"outbox_events", "outbox_dead_letters"} {
		enabled, forced := rlsFlags(t, superPool, ctx, table)
		assert.Truef(t, enabled, "%s: relrowsecurity should be true", table)
		assert.Truef(t, forced, "%s: relforcerowsecurity should be true", table)
	}
}

func TestOutboxEvents_AnOwnerRoleSeesOnlyItsOwnTenant(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	const ownerPassword = "ownerpassword123"
	for _, stmt := range []string{
		"DROP ROLE IF EXISTS outbox_owner_probe",
		fmt.Sprintf("CREATE ROLE outbox_owner_probe LOGIN PASSWORD '%s'", ownerPassword),
		"GRANT CONNECT ON DATABASE testdb TO outbox_owner_probe",
		"GRANT USAGE ON SCHEMA public TO outbox_owner_probe",
		"ALTER TABLE outbox_events OWNER TO outbox_owner_probe",
	} {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, stmt)
			return err
		}))
	}
	tenantA, tenantB := uuid.New(), uuid.New()
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events (id, event_type, payload, tenant_id) VALUES ($1, 'SLAWarning', '{}', $2)`, uuid.New(), tenantA.String())
		return err
	}))
	ownerPool, err := pgcommon.NewPool(ctx, pgcommon.Config{
		DSN:         fmt.Sprintf("postgres://outbox_owner_probe:%s@%s", ownerPassword, hostAndDB(superDSN)),
		GUCProvider: pgcommon.GUCSetFromContext,
	})
	require.NoError(t, err)
	t.Cleanup(ownerPool.Close)

	count := func(ctx context.Context) (n int) {
		require.NoError(t, ownerPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE event_type = 'SLAWarning'").Scan(&n)
		}))
		return n
	}
	assert.Equal(t, 1, count(withGUC(ctx, tenantA)), "the owner sees its own tenant's events")
	assert.Zero(t, count(withGUC(ctx, tenantB)), "the owner of the table does not bypass the tenant policy")
	assert.Zero(t, count(ctx), "an owner connection with no tenant sees nothing")
}
