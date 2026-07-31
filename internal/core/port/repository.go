package port

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// Cursor and PageRequest are T1.2's inferred keyset-pagination shape (LLD
// §5.9) — a placeholder until T1.1's own core/port interfaces land.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type PageRequest struct {
	After *Cursor
	Limit int
}

// InstanceRepository, TaskRepository, and TaskAssignmentRepository are T1.2's
// own best-effort inference of the persistence-layer contract — T1.1 owns
// the real interfaces; expect a mechanical reconciliation diff once they
// land. GetByBusinessKey is deliberately omitted: no backing query exists
// yet in db/queries.
type InstanceRepository interface {
	Create(ctx context.Context, inst *domain.Instance) error
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error)
	UpdateStatus(
		ctx context.Context,
		tenantID, id uuid.UUID,
		status domain.InstanceStatus,
		recordVersion int64,
	) (*domain.Instance, error)
	ListByTenant(ctx context.Context, tenantID uuid.UUID, page PageRequest) ([]*domain.Instance, *Cursor, error)
}

type TaskRepository interface {
	Create(ctx context.Context, task *domain.Task) error
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error)
	UpdateStatus(
		ctx context.Context,
		tenantID, id uuid.UUID,
		status domain.TaskStatus,
		recordVersion int64,
	) (*domain.Task, error)
	ListByInstance(
		ctx context.Context,
		tenantID, instanceID uuid.UUID,
		page PageRequest,
	) ([]*domain.Task, *Cursor, error)
}

type TaskAssignmentRepository interface {
	Create(ctx context.Context, assignment *domain.TaskAssignment) error
	ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error)
	ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error)
	Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error)
}
