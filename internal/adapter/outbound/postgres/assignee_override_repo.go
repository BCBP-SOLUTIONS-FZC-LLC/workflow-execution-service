package postgres

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.AssigneeOverrideRepository = (*AssigneeOverrideRepo)(nil)

type AssigneeOverrideRepo struct {
	pool *pgcommon.Pool
}

func NewAssigneeOverrideRepo(pool *pgcommon.Pool) *AssigneeOverrideRepo {
	return &AssigneeOverrideRepo{pool: pool}
}

func (r *AssigneeOverrideRepo) Create(ctx context.Context, override *domain.AssigneeOverride) error {
	ctx = withTenantGUC(ctx, override.TenantID)
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).CreateAssigneeOverride(ctx, db.CreateAssigneeOverrideParams{
			ID:                 override.ID,
			TenantID:           override.TenantID,
			WorkflowInstanceID: override.WorkflowInstanceID,
			NodeKey:            override.NodeKey,
			PreviousUserID:     override.PreviousUserID,
			NewUserID:          override.NewUserID,
			Reason:             toNullableText(override.Reason),
			ActorUserID:        override.ActorUserID,
		})
		if err != nil {
			return mapErr(err)
		}
		*override = *assigneeOverrideFromDB(row)
		return nil
	})
}

func (r *AssigneeOverrideRepo) ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID) ([]*domain.AssigneeOverride, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var overrides []*domain.AssigneeOverride
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListAssigneeOverridesByInstance(ctx, instanceID)
		if err != nil {
			return mapErr(err)
		}
		overrides = make([]*domain.AssigneeOverride, len(rows))
		for i, row := range rows {
			overrides[i] = assigneeOverrideFromDB(row)
		}
		return nil
	})
	return overrides, err
}
