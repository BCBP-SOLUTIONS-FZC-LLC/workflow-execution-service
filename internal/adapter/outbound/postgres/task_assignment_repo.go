package postgres

import (
	"context"
	"encoding/json"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TaskAssignmentRepository = (*TaskAssignmentRepo)(nil)

type TaskAssignmentRepo struct {
	pool *pgcommon.Pool
}

func NewTaskAssignmentRepo(pool *pgcommon.Pool) *TaskAssignmentRepo {
	return &TaskAssignmentRepo{pool: pool}
}

func (r *TaskAssignmentRepo) Create(ctx context.Context, a *domain.TaskAssignment) error {
	ctx = withTenantGUC(ctx, a.TenantID)
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).CreateWorkflowTaskAssignment(ctx, db.CreateWorkflowTaskAssignmentParams{
			ID:         a.ID,
			TenantID:   a.TenantID,
			TaskID:     a.TaskID,
			UserID:     a.UserID,
			AssignedBy: toPgtypeUUID(a.AssignedBy),
			Reason:     toNullableText(a.Reason),
			IsLead:     a.IsLead,
			AssignedAt: toPgtypeTimestamptz(a.AssignedAt),
		})
		if err != nil {
			return mapErr(err)
		}
		*a = *taskAssignmentFromDB(row)
		return nil
	})
}

func (r *TaskAssignmentRepo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignment *domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).GetWorkflowTaskAssignment(ctx, id)
		if err != nil {
			return mapErr(err)
		}
		assignment = taskAssignmentFromDB(row)
		return nil
	})
	return assignment, err
}

func (r *TaskAssignmentRepo) ListActiveByTask(
	ctx context.Context,
	tenantID, taskID uuid.UUID,
) ([]*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignments []*domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListActiveAssignmentsByTask(ctx, taskID)
		if err != nil {
			return mapErr(err)
		}
		assignments = make([]*domain.TaskAssignment, len(rows))
		for i, row := range rows {
			assignments[i] = taskAssignmentFromDB(row)
		}
		return nil
	})
	return assignments, err
}

func (r *TaskAssignmentRepo) ListActiveByUser(
	ctx context.Context,
	tenantID, userID uuid.UUID,
) ([]*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignments []*domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListActiveAssignmentsByUser(ctx, db.ListActiveAssignmentsByUserParams{
			TenantID: tenantID,
			UserID:   userID,
		})
		if err != nil {
			return mapErr(err)
		}
		assignments = make([]*domain.TaskAssignment, len(rows))
		for i, row := range rows {
			assignments[i] = taskAssignmentFromDB(row)
		}
		return nil
	})
	return assignments, err
}

func (r *TaskAssignmentRepo) Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignment *domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).VacateWorkflowTaskAssignment(ctx, id)
		if err != nil {
			return mapErr(err)
		}
		assignment = taskAssignmentFromDB(row)
		return nil
	})
	return assignment, err
}

func (r *TaskAssignmentRepo) Complete(
	ctx context.Context,
	tenantID, id uuid.UUID,
	resultJSON json.RawMessage,
) (*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignment *domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).CompleteWorkflowTaskAssignment(ctx, db.CompleteWorkflowTaskAssignmentParams{
			ID:         id,
			ResultJson: resultJSON,
		})
		if err != nil {
			return mapErr(err)
		}
		assignment = taskAssignmentFromDB(row)
		return nil
	})
	return assignment, err
}

func (r *TaskAssignmentRepo) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID) (*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignment *domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		if err := q.ClearOtherTaskAssignmentLeads(ctx, db.ClearOtherTaskAssignmentLeadsParams{
			TaskID: taskID,
			ID:     id,
		}); err != nil {
			return mapErr(err)
		}
		row, err := q.SetTaskAssignmentLead(ctx, id)
		if err != nil {
			return mapErr(err)
		}
		assignment = taskAssignmentFromDB(row)
		return nil
	})
	return assignment, err
}
