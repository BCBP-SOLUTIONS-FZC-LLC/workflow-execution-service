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

var _ port.UserAvailabilityRepository = (*UserAvailabilityRepo)(nil)

type UserAvailabilityRepo struct {
	pool *pgcommon.Pool
}

func NewUserAvailabilityRepo(pool *pgcommon.Pool) *UserAvailabilityRepo {
	return &UserAvailabilityRepo{pool: pool}
}

func (r *UserAvailabilityRepo) Record(ctx context.Context, tenantID, userID uuid.UUID, status string, changedAt time.Time) error {
	ctx = withTenantGUC(ctx, tenantID)
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return mapErr(db.New(dbtx).UpsertUserAvailability(ctx, db.UpsertUserAvailabilityParams{
			TenantID: tenantID, UserID: userID, Status: db.UserAvailabilityStatus(status), ChangedAt: changedAt,
		}))
	})
}

func (r *UserAvailabilityRepo) Get(ctx context.Context, tenantID, userID uuid.UUID) (string, bool, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var status string
	var found bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		s, err := db.New(dbtx).GetUserAvailability(ctx, db.GetUserAvailabilityParams{TenantID: tenantID, UserID: userID})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return mapErr(err)
		}
		status, found = string(s), true
		return nil
	})
	return status, found, err
}
