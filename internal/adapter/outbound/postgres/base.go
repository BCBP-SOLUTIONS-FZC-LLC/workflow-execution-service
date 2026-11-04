package postgres

import (
	"context"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
)

func exec(ctx context.Context, pool *pgcommon.Pool, fn func(db.DBTX) error) error {
	if tx, ok := txFromContext(ctx); ok {
		return fn(tx)
	}
	return pool.WithConn(ctx, func(_ context.Context, conn *pgxpool.Conn) error { //nolint:wrapcheck // the pool's error is the caller's to classify
		return fn(conn)
	})
}

func withTenantGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID.String()})
}
