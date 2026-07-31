package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Task is one dispatched stage/task row (LLD §4.3).
type Task struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	WorkflowInstanceID uuid.UUID
	NodeKey            string
	DepartmentID       uuid.UUID
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
