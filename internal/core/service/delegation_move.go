package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func activeAssignmentByID(active []*domain.TaskAssignment, id uuid.UUID) bool {
	for _, a := range active {
		if a.ID == id {
			return true
		}
	}
	return false
}

func activeAssignmentOfUser(active []*domain.TaskAssignment, userID uuid.UUID) *domain.TaskAssignment {
	for _, a := range active {
		if a.UserID == userID {
			return a
		}
	}
	return nil
}

// vacateIfActive vacates the assignment inside the caller's transaction, after
// re-reading the task's active assignments there: the rows were read earlier,
// outside the transaction, and the vacate statement does not check that the
// row is still active. It returns the task's active assignments and false,
// with nothing written, when the assignment has already ended.
func vacateIfActive(ctx context.Context, repo port.TaskAssignmentRepository, tenantID uuid.UUID, a *domain.TaskAssignment) ([]*domain.TaskAssignment, bool, error) {
	active, err := repo.ListActiveByTask(ctx, tenantID, a.TaskID)
	if err != nil {
		return nil, false, err
	}
	if !activeAssignmentByID(active, a.ID) {
		return nil, false, nil
	}
	if _, err := repo.Vacate(ctx, tenantID, a.ID); err != nil {
		return nil, false, err
	}
	return active, true, nil
}

// handOver gives the vacated assignment's task to newUserID inside the caller's
// transaction. A user who already holds the task keeps that one row, and
// inherits the lead flag; inserting a second row would be a unique violation,
// which aborts the whole transaction.
func handOver(ctx context.Context, repo port.TaskAssignmentRepository, tenantID uuid.UUID, vacated *domain.TaskAssignment, active []*domain.TaskAssignment, task *domain.Task, newUserID uuid.UUID, reason string) error {
	if existing := activeAssignmentOfUser(active, newUserID); existing != nil {
		if !vacated.IsLead {
			return nil
		}
		_, err := repo.SetLead(ctx, tenantID, task.ID, existing.ID, task.RecordVersion)
		return err
	}
	return repo.Create(ctx, &domain.TaskAssignment{
		ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: newUserID,
		Reason: reason, IsLead: vacated.IsLead,
	})
}
