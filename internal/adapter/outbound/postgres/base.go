// Package postgres implements execution-service's core/port repository
// interfaces against Postgres.
//
// Methods that take an explicit tenantID set the app.tenant_id GUC
// themselves (LLD §9.2) — but only for a standalone call with no ambient
// transaction. Transactor.RunInTx/RunInTxWithRetry acquire the connection
// and begin the transaction using the ctx they're given before their
// callback ever runs; a repo method's own GUC re-assertion inside that
// callback comes too late to affect a connection already acquired. Any
// caller composing multiple repo calls inside one RunInTx (including
// OutboxRepository.Enqueue, which takes no tenantID at all) must set the GUC
// on ctx before calling RunInTx, not rely on a repo method to do it from
// inside the callback.
package postgres

import (
	"context"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
)

func exec(ctx context.Context, pool *pgcommon.Pool, fn func(db.DBTX) error) error {
	if tx, ok := txFromContext(ctx); ok {
		return fn(tx)
	}
	return pool.WithConn(ctx, func(_ context.Context, conn *pgxpool.Conn) error { //nolint:wrapcheck
		return fn(conn)
	})
}

func withTenantGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID.String()})
}
