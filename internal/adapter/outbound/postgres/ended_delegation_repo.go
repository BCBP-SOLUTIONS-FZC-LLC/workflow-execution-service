package postgres

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.EndedDelegationRepository = (*EndedDelegationRepo)(nil)

type EndedDelegationRepo struct {
	pool *pgcommon.Pool
}

func NewEndedDelegationRepo(pool *pgcommon.Pool) *EndedDelegationRepo {
	return &EndedDelegationRepo{pool: pool}
}

func (r *EndedDelegationRepo) MarkEnded(ctx context.Context, tenantID, delegationID uuid.UUID) error {
	ctx = withTenantGUC(ctx, tenantID)
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return mapErr(db.New(dbtx).MarkDelegationEnded(ctx, db.MarkDelegationEndedParams{TenantID: tenantID, DelegationID: delegationID}))
	})
}

func (r *EndedDelegationRepo) IsEnded(ctx context.Context, tenantID, delegationID uuid.UUID) (bool, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var ended bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).DelegationIsEnded(ctx, db.DelegationIsEndedParams{TenantID: tenantID, DelegationID: delegationID})
		if err != nil {
			return mapErr(err)
		}
		ended = v
		return nil
	})
	return ended, err
}
