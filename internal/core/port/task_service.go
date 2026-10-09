package port

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskStatus string

const (
	TaskStatusReady      TaskStatus = "READY"
	TaskStatusInProgress TaskStatus = "IN_PROGRESS"
	TaskStatusCompleted  TaskStatus = "COMPLETED"
	TaskStatusDeferred   TaskStatus = "DEFERRED"
	TaskStatusFailed     TaskStatus = "FAILED"
	TaskStatusSuperseded TaskStatus = "SUPERSEDED"
)

type Task struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	WorkflowInstanceID uuid.UUID
	NodeKey            string
	TaskType           string
	DepartmentID       uuid.UUID
	RequiredLevel      string
	Status             TaskStatus
	RecordVersion      int64
	AssigneeMode       string
	AssigneeCount      int
	ConnectorType      *string
	ExtrasJSON         json.RawMessage
	DeferredFromTaskID *uuid.UUID
	DueAt              *time.Time
	FollowUpAt         *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        *time.Time
}

type TaskAssignment struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	TaskID      uuid.UUID
	UserID      uuid.UUID
	AssignedBy  *uuid.UUID
	Reason      string
	IsLead      bool
	IsActive    bool
	AssignedAt  *time.Time
	ClaimedAt   *time.Time
	CompletedAt *time.Time
	ResultJSON  json.RawMessage
	VacatedAt   *time.Time
	UpdatedAt   time.Time
}

type ActiveUserTask struct {
	TaskID             uuid.UUID
	WorkflowInstanceID uuid.UUID
	NodeKey            string
	UserID             uuid.UUID
	DepartmentID       uuid.UUID
	Status             TaskStatus
	RecordVersion      int64
}

type TaskFilter struct {
	Status             *TaskStatus
	WorkflowInstanceID *uuid.UUID
	DepartmentID       *uuid.UUID
	AssigneeUserID     *uuid.UUID
	DueBefore          *time.Time
}

type ReadScope struct {
	CallerUserID uuid.UUID
	Departments  []DepartmentRole
	IsAdmin      bool
}

type DepartmentRole struct {
	DepartmentID uuid.UUID
	Role         string
}

type Page struct {
	Cursor *CursorPosition
	Limit  int
}

type PageResult[T any] struct {
	Items      []T
	NextCursor string
}

type AssigneeOverrideInput struct {
	TenantID      uuid.UUID
	InstanceID    uuid.UUID
	NodeKey       string
	NewUserID     uuid.UUID
	Reason        string
	ActorUserID   uuid.UUID
	RecordVersion int64
}

type AssigneeOverride struct {
	ID                 uuid.UUID
	WorkflowInstanceID uuid.UUID
	NodeKey            string
	PreviousUserID     uuid.UUID
	NewUserID          uuid.UUID
	Reason             string
	ActorUserID        uuid.UUID
	RecordVersion      int64
	CreatedAt          time.Time
}

type TaskService interface {
	List(ctx context.Context, tenantID uuid.UUID, scope ReadScope, filter TaskFilter, page Page) (PageResult[*Task], error)
	Get(ctx context.Context, tenantID, taskID uuid.UUID, scope ReadScope) (*Task, []*TaskAssignment, error)
	GetByNode(ctx context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*Task, error)

	Claim(ctx context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*Task, error)
	Complete(ctx context.Context, tenantID, taskID, userID uuid.UUID, resultJSON json.RawMessage, recordVersion int64) (*Task, error)
	Defer(ctx context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*Task, error)
	Reassign(ctx context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, recordVersion int64) (*Task, error)

	OverrideAssignee(ctx context.Context, in AssigneeOverrideInput) (*AssigneeOverride, error)
	SignalReassign(ctx context.Context, tenantID, taskID, actorUserID, previousUserID, newUserID uuid.UUID, recordVersion int64) error

	ActiveByUser(ctx context.Context, tenantID, userID uuid.UUID, page Page) (PageResult[*ActiveUserTask], error)
}
