//go:build integration

package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const relayRoleTestPassword = "rolecheckpassword1"

func roleCheckPool(t *testing.T, superPool *pgcommon.Pool, superDSN, role, attributes string) *pgcommon.Pool {
	t.Helper()
	ctx := context.Background()
	for _, stmt := range []string{
		"DROP ROLE IF EXISTS " + role,
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s' %s", role, relayRoleTestPassword, attributes),
		fmt.Sprintf("GRANT CONNECT ON DATABASE testdb TO %s", role),
	} {
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, stmt)
			return err
		}))
	}
	u, err := url.Parse(superDSN)
	require.NoError(t, err)
	hostAndDB := strings.TrimPrefix(superDSN, u.Scheme+"://"+u.User.String()+"@")
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: fmt.Sprintf("postgres://%s:%s@%s", role, relayRoleTestPassword, hostAndDB)})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestRoleBypassesRLS_ReadsTheConnectionsOwnRole(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	cases := []struct {
		name       string
		pool       func() *pgcommon.Pool
		wantBypass bool
	}{
		{"superuser", func() *pgcommon.Pool { return superPool }, true},
		{"role with BYPASSRLS", func() *pgcommon.Pool { return roleCheckPool(t, superPool, superDSN, "rolecheck_relay", "BYPASSRLS") }, true},
		{"plain application role", func() *pgcommon.Pool { return roleCheckPool(t, superPool, superDSN, "rolecheck_app", "NOBYPASSRLS") }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := roleBypassesRLS(tc.pool())(context.Background())

			require.NoError(t, err)
			assert.Equal(t, tc.wantBypass, got)
		})
	}
}
