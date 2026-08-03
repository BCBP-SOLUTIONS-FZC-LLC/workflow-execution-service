// Package port defines the interfaces internal/adapter code depends on and
// internal/core/service will implement.
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
	DepartmentID       uuid.UUID
	// RequiredLevel is the node's eligibility-level requirement, supplied to
	// the outbound eligibility check alongside DepartmentID (LLD §5.4 step 2).
	RequiredLevel      string
	Status             TaskStatus
	RecordVersion      int64
	AssigneeMode       string
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
}

// ReadScope is the intra-tenant read-scope check's input (LLD §9.2): a
// caller may read a task only if they are (or were) an assignee, their
// current departments include the resource's, or IsAdmin bypasses the check.
type ReadScope struct {
	CallerUserID uuid.UUID
	Departments  []string
	IsAdmin      bool
}

// Page is a list endpoint's decoded, validated pagination input. Cursor is
// nil on a first page — by the time a Page reaches this port boundary, an
// incoming cursor has already been decoded and validated (CursorPosition),
// never a raw, unchecked string (LLD §5.9's "unparseable or tampered cursor
// is rejected outright" requirement).
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

// TaskService is the port internal/adapter/inbound/http/handler codes
// against. Every mutating method performs its own synchronous
// record_version/state pre-check before forwarding a signal (LLD §5.10).
type TaskService interface {
	List(ctx context.Context, tenantID uuid.UUID, scope ReadScope, filter TaskFilter, page Page) (PageResult[*Task], error)
	Get(ctx context.Context, tenantID, taskID uuid.UUID, scope ReadScope) (*Task, []*TaskAssignment, error)
	GetByNode(ctx context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*Task, error)

	Claim(ctx context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*Task, error)
	Complete(ctx context.Context, tenantID, taskID, userID uuid.UUID, resultJSON json.RawMessage, recordVersion int64) (*Task, error)
	Defer(ctx context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*Task, error)
	Reassign(ctx context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, recordVersion int64) (*Task, error)

	// OverrideAssignee performs LLD §5.4 step 1 (validate + version check)
	// and step 3 (persist); the caller runs step 2 (eligibility) first and
	// fires step 4 (signal) only after this call succeeds.
	OverrideAssignee(ctx context.Context, in AssigneeOverrideInput) (*AssigneeOverride, error)
	SignalReassign(ctx context.Context, tenantID, taskID, actorUserID, previousUserID, newUserID uuid.UUID, recordVersion int64) error

	ActiveByUser(ctx context.Context, tenantID, userID uuid.UUID, page Page) (PageResult[*ActiveUserTask], error)
}
