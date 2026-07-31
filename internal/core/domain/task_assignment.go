package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// TaskAssignment is one assignee on a task (LLD §4.4). A row is never
// deleted on reassignment, only vacated and superseded by a fresh insert.
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
