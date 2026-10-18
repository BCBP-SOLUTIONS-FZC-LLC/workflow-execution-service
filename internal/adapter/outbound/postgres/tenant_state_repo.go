package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TenantStateRepository = (*TenantStateRepo)(nil)

type TenantStateRepo struct {
	pool *pgcommon.Pool
}

func NewTenantStateRepo(pool *pgcommon.Pool) *TenantStateRepo {
	return &TenantStateRepo{pool: pool}
}

func (r *TenantStateRepo) Record(ctx context.Context, tenantID uuid.UUID, status string, changedAt time.Time) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return mapErr(db.New(dbtx).UpsertTenantState(ctx, db.UpsertTenantStateParams{TenantID: tenantID, Status: status, ChangedAt: changedAt}))
	})
}

func (r *TenantStateRepo) Get(ctx context.Context, tenantID uuid.UUID) (string, bool, error) {
	var status string
	var found bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		s, err := db.New(dbtx).GetTenantState(ctx, tenantID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return mapErr(err)
		}
		status, found = s, true
		return nil
	})
	return status, found, err
}
