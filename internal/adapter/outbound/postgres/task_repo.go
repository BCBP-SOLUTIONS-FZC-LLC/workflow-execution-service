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

var _ port.TaskRepository = (*TaskRepo)(nil)

type TaskRepo struct {
	pool *pgcommon.Pool
}

func NewTaskRepo(pool *pgcommon.Pool) *TaskRepo {
	return &TaskRepo{pool: pool}
}

func (r *TaskRepo) Create(ctx context.Context, task *domain.Task) error {
	ctx = withTenantGUC(ctx, task.TenantID)
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).CreateWorkflowTask(ctx, db.CreateWorkflowTaskParams{
			ID:                 task.ID,
			TenantID:           task.TenantID,
			WorkflowInstanceID: task.WorkflowInstanceID,
			NodeKey:            task.NodeKey,
			DepartmentID:       task.DepartmentID,
			Status:             db.WorkflowTaskStatus(task.Status),
			AssigneeMode:       task.AssigneeMode,
			DueAt:              toPgtypeTimestamptz(task.DueAt),
		})
		if err != nil {
			return mapErr(err)
		}
		*task = *taskFromDB(row)
		return nil
	})
}

func (r *TaskRepo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var task *domain.Task
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).GetWorkflowTask(ctx, id)
		if err != nil {
			return mapErr(err)
		}
		task = taskFromDB(row)
		return nil
	})
	return task, err
}

func (r *TaskRepo) UpdateStatus(
	ctx context.Context,
	tenantID, id uuid.UUID,
	status domain.TaskStatus,
	recordVersion int64,
) (*domain.Task, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var task *domain.Task
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		row, err := q.UpdateWorkflowTaskStatus(ctx, db.UpdateWorkflowTaskStatusParams{
			ID:            id,
			Status:        db.WorkflowTaskStatus(status),
			RecordVersion: recordVersion,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			_, probeErr := q.GetWorkflowTask(ctx, id)
			return notFoundOrVersionConflict(probeErr)
		}
		if err != nil {
			return mapErr(err)
		}
		task = taskFromDB(row)
		return nil
	})
	return task, err
}

func (r *TaskRepo) ListByInstance(
	ctx context.Context,
	tenantID, instanceID uuid.UUID,
	page port.PageRequest,
) ([]*domain.Task, *port.Cursor, error) {
	ctx = withTenantGUC(ctx, tenantID)
	limit := clampLimit(page.Limit)

	params := db.ListWorkflowTasksByInstanceParams{
		WorkflowInstanceID: instanceID,
		Limit:              int32(limit + 1), //nolint:gosec
	}
	if page.After != nil {
		params.CursorCreatedAt = toPgtypeTimestamptz(&page.After.CreatedAt)
		params.CursorID = toPgtypeUUID(&page.After.ID)
	}

	var tasks []*domain.Task
	var next *port.Cursor
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListWorkflowTasksByInstance(ctx, params)
		if err != nil {
			return mapErr(err)
		}
		trimmed, cursor := paginate(rows, limit, func(row db.WorkflowTask) (time.Time, uuid.UUID) {
			return row.CreatedAt, row.ID
		})
		next = cursor
		tasks = make([]*domain.Task, len(trimmed))
		for i, row := range trimmed {
			tasks[i] = taskFromDB(row)
		}
		return nil
	})
	return tasks, next, err
}
