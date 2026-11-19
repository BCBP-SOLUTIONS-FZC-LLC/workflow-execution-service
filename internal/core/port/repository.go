package port

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type PageRequest struct {
	After *Cursor
	Limit int
}

type ScopeFilter struct {
	DepartmentIDs []uuid.UUID
	CallerUserID  uuid.UUID
}

type InstanceListFilter struct {
	InstanceID        *uuid.UUID
	Status            *domain.InstanceStatus
	Statuses          []domain.InstanceStatus
	SubjectID         *string
	WorkflowVersionID *uuid.UUID
	StartedAfter      *time.Time
	StartedBefore     *time.Time
	Scope             *ScopeFilter
}

type InstanceRepository interface {
	Create(ctx context.Context, inst *domain.Instance) error
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error)
	GetLiveByBusinessKey(ctx context.Context, tenantID uuid.UUID, subjectID, businessKey string) (*domain.Instance, error)
	RecordRunID(ctx context.Context, tenantID, id uuid.UUID, runID string) error
	UpdateStatus(
		ctx context.Context,
		tenantID, id uuid.UUID,
		status domain.InstanceStatus,
		recordVersion int64,
	) (*domain.Instance, error)
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
	CountActiveByWorkflow(ctx context.Context, tenantID, workflowID uuid.UUID) (int64, error)
}

type TaskListFilter struct {
	Status             *domain.TaskStatus
	WorkflowInstanceID *uuid.UUID
	DepartmentID       *uuid.UUID
	AssigneeUserID     *uuid.UUID
	DueBefore          *time.Time
	Scope              *ScopeFilter
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
	ListByTenant(
		ctx context.Context,
		tenantID uuid.UUID,
		filter TaskListFilter,
		page PageRequest,
	) ([]*domain.Task, *Cursor, error)
	GetByInstanceAndNode(ctx context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*domain.Task, error)
	BumpRecordVersion(ctx context.Context, tenantID, id uuid.UUID, recordVersion int64) (*domain.Task, error)
}

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
	// ListByTaskAndUser returns every assignment the user holds or held on the task, closed rows
	// included, whichever way the row was created; none is an empty list.
	ListByTaskAndUser(ctx context.Context, tenantID, taskID, userID uuid.UUID) ([]*domain.TaskAssignment, error)
	ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error)
	// ListActiveByReasonTag returns the tenant's active assignments whose reason contains the
	// tag, whoever holds them; the caller checks that the tag stands on its own in the reason.
	ListActiveByReasonTag(ctx context.Context, tenantID uuid.UUID, tag string) ([]*domain.TaskAssignment, error)
	// CountActiveByTasks returns the number of active assignments of each task in one query; a task
	// with none is absent from the map.
	CountActiveByTasks(ctx context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error)
	ListActiveByUserPaginated(
		ctx context.Context,
		tenantID, userID uuid.UUID,
		page PageRequest,
	) ([]ActiveUserTaskRow, *Cursor, error)
	Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error)
	// VacateActive vacates the assignment only while it is still active and returns
	// domain.ErrNotFound when it has already ended.
	VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error)
	VacateAllActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error)
	Complete(ctx context.Context, tenantID, id uuid.UUID, resultJSON json.RawMessage, taskRecordVersion int64) (*domain.TaskAssignment, error)
	// SetLead makes the assignment the task's only lead, whoever moved it there.
	SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, taskRecordVersion int64) (*domain.TaskAssignment, error)
	// ClaimLead is SetLead for the assignee claiming the task: it also records when the
	// assignee claimed it, keeping the first time.
	ClaimLead(ctx context.Context, tenantID, taskID, id uuid.UUID, taskRecordVersion int64) (*domain.TaskAssignment, error)
}

type AssigneeOverrideRepository interface {
	Create(ctx context.Context, override *domain.AssigneeOverride) error
	ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID) ([]*domain.AssigneeOverride, error)
}

type ActiveTaskQueueRepository interface {
	ListActive(ctx context.Context) ([]*domain.ActiveTaskQueue, error)
	GetByQueueName(ctx context.Context, queueName string) (*domain.ActiveTaskQueue, error)
	Register(ctx context.Context, tenantID uuid.UUID, queueName string) (*domain.ActiveTaskQueue, error)
	StopRouting(ctx context.Context, queueName string) error
}
