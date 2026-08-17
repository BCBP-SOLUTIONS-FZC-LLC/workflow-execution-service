package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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

func (r *TaskAssignmentRepo) ListActiveByUserPaginated(
	ctx context.Context,
	tenantID, userID uuid.UUID,
	page port.PageRequest,
) ([]port.ActiveUserTaskRow, *port.Cursor, error) {
	ctx = withTenantGUC(ctx, tenantID)
	limit := clampLimit(page.Limit)

	params := db.ListActiveTasksByUserParams{
		TenantID: tenantID,
		UserID:   userID,
		Limit:    int32(limit + 1), //nolint:gosec
	}
	if page.After != nil {
		params.CursorCreatedAt = toPgtypeTimestamptz(&page.After.CreatedAt)
		params.CursorID = toPgtypeUUID(&page.After.ID)
	}

	var out []port.ActiveUserTaskRow
	var next *port.Cursor
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListActiveTasksByUser(ctx, params)
		if err != nil {
			return mapErr(err)
		}
		trimmed, cursor := paginate(rows, limit, func(row db.ListActiveTasksByUserRow) (time.Time, uuid.UUID) {
			return row.CreatedAt, row.TaskID
		})
		next = cursor
		out = make([]port.ActiveUserTaskRow, len(trimmed))
		for i, row := range trimmed {
			out[i] = port.ActiveUserTaskRow{
				TaskID:             row.TaskID,
				WorkflowInstanceID: row.WorkflowInstanceID,
				NodeKey:            row.NodeKey,
				UserID:             row.UserID,
				DepartmentID:       row.DepartmentID,
				Status:             domain.TaskStatus(row.Status),
				RecordVersion:      row.RecordVersion,
				CreatedAt:          row.CreatedAt,
			}
		}
		return nil
	})
	return out, next, err
}

func (r *TaskAssignmentRepo) VacateAllActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignments []*domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).VacateAllActiveAssignmentsByUser(ctx, db.VacateAllActiveAssignmentsByUserParams{
			TenantID: tenantID, UserID: userID,
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
	taskRecordVersion int64,
) (*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignment *domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		row, err := q.CompleteWorkflowTaskAssignment(ctx, db.CompleteWorkflowTaskAssignmentParams{
			ID:         id,
			ResultJson: resultJSON,
		})
		if err != nil {
			return mapErr(err)
		}
		if err := bumpTaskRecordVersion(ctx, q, row.TaskID, taskRecordVersion); err != nil {
			return err
		}
		assignment = taskAssignmentFromDB(row)
		return nil
	})
	return assignment, err
}

func (r *TaskAssignmentRepo) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, taskRecordVersion int64) (*domain.TaskAssignment, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var assignment *domain.TaskAssignment
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		if err := bumpTaskRecordVersion(ctx, q, taskID, taskRecordVersion); err != nil {
			return err
		}
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

// bumpTaskRecordVersion is Complete/SetLead's optimistic-concurrency guard:
// the LLD frames workflow_task.record_version, not the assignment (which has
// no version column of its own), as claim/complete's contested resource. A
// zero-row result is disambiguated the same way UpdateWorkflowTaskStatus's
// own conflict path already does (notFoundOrVersionConflict).
func bumpTaskRecordVersion(ctx context.Context, q *db.Queries, taskID uuid.UUID, recordVersion int64) error {
	_, err := q.BumpWorkflowTaskRecordVersion(ctx, db.BumpWorkflowTaskRecordVersionParams{
		ID:            taskID,
		RecordVersion: recordVersion,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		_, probeErr := q.GetWorkflowTask(ctx, taskID)
		return notFoundOrVersionConflict(probeErr)
	}
	if err != nil {
		return mapErr(err)
	}
	return nil
}
