package port

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// Cursor and PageRequest are this persistence layer's keyset-pagination
// shape (LLD §5.9).
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type PageRequest struct {
	After *Cursor
	Limit int
}

// InstanceListFilter narrows InstanceRepository.ListByTenant's query —
// domain-typed, the repo-layer counterpart to InstanceService's own
// port.InstanceFilter (service-level, port.InstanceStatus-typed); the
// service layer converts between the two.
type InstanceListFilter struct {
	Status            *domain.InstanceStatus
	WorkflowVersionID *uuid.UUID
	StartedAfter      *time.Time
	StartedBefore     *time.Time
}

// InstanceRepository, TaskRepository, and TaskAssignmentRepository are this
// persistence layer's own repository interfaces, inferred from the LLD's
// data model. GetByBusinessKey is deliberately omitted: no backing query
// exists yet in db/queries.
type InstanceRepository interface {
	Create(ctx context.Context, inst *domain.Instance) error
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error)
	UpdateStatus(
		ctx context.Context,
		tenantID, id uuid.UUID,
		status domain.InstanceStatus,
		recordVersion int64,
	) (*domain.Instance, error)
	// ListByTenant backs InstanceService.List (GET /instances).
	ListByTenant(
		ctx context.Context,
		tenantID uuid.UUID,
		filter InstanceListFilter,
		page PageRequest,
	) ([]*domain.Instance, *Cursor, error)
	UpdateCurrentNodeKeys(
		ctx context.Context,
		tenantID, id uuid.UUID,
		currentNodeKeys []string,
		recordVersion int64,
	) (*domain.Instance, error)
	// CountActiveByWorkflow backs ArchiveGuard.CheckActiveInstances(tenant_id,
	// workflow_id) — active meaning RUNNING/PAUSED/DEGRADED.
	CountActiveByWorkflow(ctx context.Context, tenantID, workflowID uuid.UUID) (int64, error)
	// CountActiveByTaskQueue backs TenantLifecycleReconciler's plan-downgrade
	// check (LLD §3.2 item 3): a tenant's isolated queue is never
	// deregistered while any instance started on it is still running.
	CountActiveByTaskQueue(ctx context.Context, tenantID uuid.UUID, taskQueue string) (int64, error)
}

// TaskListFilter narrows TaskRepository.ListByTenant's query — domain-typed,
// the repo-layer counterpart to TaskService's own port.TaskFilter (service-
// level, port.TaskStatus-typed); the service layer converts between the two.
type TaskListFilter struct {
	Status             *domain.TaskStatus
	WorkflowInstanceID *uuid.UUID
	DepartmentID       *uuid.UUID
	AssigneeUserID     *uuid.UUID
	DueBefore          *time.Time
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
	// ListByTenant backs TaskService.List (GET /tasks).
	ListByTenant(
		ctx context.Context,
		tenantID uuid.UUID,
		filter TaskListFilter,
		page PageRequest,
	) ([]*domain.Task, *Cursor, error)
	// GetByInstanceAndNode backs TaskService.GetByNode — returns the current
	// (most recently created) task at this node.
	GetByInstanceAndNode(ctx context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*domain.Task, error)
}

// ActiveUserTaskRow is TaskAssignmentRepository.ListActiveByUserPaginated's
// row shape — a task⋈assignment join projection, domain-layer counterpart
// to port.ActiveUserTask (the service layer maps between the two).
type ActiveUserTaskRow struct {
	TaskID             uuid.UUID
	WorkflowInstanceID uuid.UUID
	NodeKey            string
	UserID             uuid.UUID
	DepartmentID       uuid.UUID
	Status             domain.TaskStatus
	RecordVersion      int64
	CreatedAt          time.Time
}

type TaskAssignmentRepository interface {
	Create(ctx context.Context, assignment *domain.TaskAssignment) error
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error)
	ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error)
	ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error)
	// ListActiveByUserPaginated backs TaskService.ActiveByUser — a
	// keyset-paginated join against the assigned task's own created_at/id
	// (workflow_task_assignment carries no created_at of its own).
	ListActiveByUserPaginated(
		ctx context.Context,
		tenantID, userID uuid.UUID,
		page PageRequest,
	) ([]ActiveUserTaskRow, *Cursor, error)
	Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error)
	// VacateAllActiveByUser backs UserSafetyNetReconciler.VacateAssignments —
	// per-assignment, tenant-wide, no scope filter (LLD §6.2 item 3).
	VacateAllActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error)
	// Complete and SetLead both take taskRecordVersion — the LLD frames the
	// task (workflow_task.record_version), not the assignment (which carries
	// no version column of its own), as claim/complete's contested resource.
	Complete(ctx context.Context, tenantID, id uuid.UUID, resultJSON json.RawMessage, taskRecordVersion int64) (*domain.TaskAssignment, error)
	SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, taskRecordVersion int64) (*domain.TaskAssignment, error)
}

// AssigneeOverrideRepository backs TaskService.OverrideAssignee's persist
// step (LLD §5.4 step 3) — insert-only, no version-checked update.
type AssigneeOverrideRepository interface {
	Create(ctx context.Context, override *domain.AssigneeOverride) error
	ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID) ([]*domain.AssigneeOverride, error)
}

// ActiveTaskQueueRepository backs the Worker's task-queue topology registry
// (LLD §4.6, §3.2) — no RLS (see db/migrations' own note), a Worker process
// legitimately reads every tenant's registered queue in one query.
type ActiveTaskQueueRepository interface {
	ListActive(ctx context.Context) ([]*domain.ActiveTaskQueue, error)
	GetByQueueName(ctx context.Context, queueName string) (*domain.ActiveTaskQueue, error)
	// Register upserts on queue_name conflict, bumping updated_at — the
	// tenant-lifecycle plan-change handler's own idempotent registration path.
	Register(ctx context.Context, tenantID uuid.UUID, queueName string) (*domain.ActiveTaskQueue, error)
	Deregister(ctx context.Context, queueName string) error
}
