package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.InstanceRepository = (*InstanceRepo)(nil)

type InstanceRepo struct {
	pool *pgcommon.Pool
}

func NewInstanceRepo(pool *pgcommon.Pool) *InstanceRepo {
	return &InstanceRepo{pool: pool}
}

func (r *InstanceRepo) Create(ctx context.Context, inst *domain.Instance) error {
	ctx = withTenantGUC(ctx, inst.TenantID)
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).CreateWorkflowInstance(ctx, db.CreateWorkflowInstanceParams{
			ID:                 inst.ID,
			TenantID:           inst.TenantID,
			WorkflowID:         inst.WorkflowID,
			WorkflowVersionID:  inst.WorkflowVersionID,
			BusinessKey:        inst.BusinessKey,
			TemporalWorkflowID: inst.TemporalWorkflowID,
			TemporalRunID:      toNullableText(inst.TemporalRunID),
			Status:             db.WorkflowInstanceStatus(inst.Status),
			CurrentNodeKeys:    inst.CurrentNodeKeys,
			TaskQueue:          inst.TaskQueue,
			StartedByUserID:    inst.StartedByUserID,
			StartedAt:          toPgtypeTimestamptz(inst.StartedAt),
		})
		if err != nil {
			return mapErr(err)
		}
		*inst = *instanceFromDB(row)
		return nil
	})
}

func (r *InstanceRepo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var inst *domain.Instance
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).GetWorkflowInstance(ctx, id)
		if err != nil {
			return mapErr(err)
		}
		inst = instanceFromDB(row)
		return nil
	})
	return inst, err
}

func (r *InstanceRepo) UpdateStatus(
	ctx context.Context,
	tenantID, id uuid.UUID,
	status domain.InstanceStatus,
	recordVersion int64,
) (*domain.Instance, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var inst *domain.Instance
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		row, err := q.UpdateWorkflowInstanceStatus(ctx, db.UpdateWorkflowInstanceStatusParams{
			ID:            id,
			Status:        db.WorkflowInstanceStatus(status),
			RecordVersion: recordVersion,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			_, probeErr := q.GetWorkflowInstance(ctx, id)
			return notFoundOrVersionConflict(probeErr)
		}
		if err != nil {
			return mapErr(err)
		}
		inst = instanceFromDB(row)
		return nil
	})
	return inst, err
}

func (r *InstanceRepo) ListByTenant(
	ctx context.Context,
	tenantID uuid.UUID,
	page port.PageRequest,
) ([]*domain.Instance, *port.Cursor, error) {
	ctx = withTenantGUC(ctx, tenantID)
	limit := clampLimit(page.Limit)

	params := db.ListWorkflowInstancesByTenantParams{
		TenantID: tenantID,
		Limit:    int32(limit + 1), //nolint:gosec
	}
	if page.After != nil {
		params.CursorCreatedAt = toPgtypeTimestamptz(&page.After.CreatedAt)
		params.CursorID = toPgtypeUUID(&page.After.ID)
	}

	var instances []*domain.Instance
	var next *port.Cursor
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListWorkflowInstancesByTenant(ctx, params)
		if err != nil {
			return mapErr(err)
		}
		trimmed, cursor := paginate(rows, limit, func(row db.WorkflowInstance) (time.Time, uuid.UUID) {
			return row.CreatedAt, row.ID
		})
		next = cursor
		instances = make([]*domain.Instance, len(trimmed))
		for i, row := range trimmed {
			instances[i] = instanceFromDB(row)
		}
		return nil
	})
	return instances, next, err
}

func (r *InstanceRepo) UpdateCurrentNodeKeys(
	ctx context.Context,
	tenantID, id uuid.UUID,
	currentNodeKeys []string,
	recordVersion int64,
) (*domain.Instance, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var inst *domain.Instance
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		row, err := q.UpdateWorkflowInstanceCurrentNodeKeys(ctx, db.UpdateWorkflowInstanceCurrentNodeKeysParams{
			ID:              id,
			CurrentNodeKeys: currentNodeKeys,
			RecordVersion:   recordVersion,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			_, probeErr := q.GetWorkflowInstance(ctx, id)
			return notFoundOrVersionConflict(probeErr)
		}
		if err != nil {
			return mapErr(err)
		}
		inst = instanceFromDB(row)
		return nil
	})
	return inst, err
}
