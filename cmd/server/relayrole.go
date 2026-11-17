package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const relayRoleCheckSQL = "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"

func roleBypassesRLS(pool *pgcommon.Pool) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		var bypasses bool
		err := pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, relayRoleCheckSQL).Scan(&bypasses)
		})
		if err != nil {
			return false, fmt.Errorf("read the connection's role: %w", err)
		}
		return bypasses, nil
	}
}

func checkRelayRole(ctx context.Context, bypasses func(context.Context) (bool, error), cfg *config.Config, log port.Logger) error {
	ok, err := bypasses(ctx)
	if err != nil {
		return fmt.Errorf("outbox relay role check: %w", err)
	}
	if ok {
		return nil
	}
	const problem = "the outbox relay role does not bypass row-level security, so it sees no outbox rows and publishes nothing"
	if !cfg.IsDev() {
		return fmt.Errorf("%s: OUTBOX_RELAY_DATABASE_URL must name a role with BYPASSRLS", problem)
	}
	log.Warn(problem+"; set OUTBOX_RELAY_DATABASE_URL to a role with BYPASSRLS", nil)
	return nil
}
