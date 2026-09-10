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
	// VisitCount is the interpreter's own per-NodeKey taskVisits counter at
	// the time this task was created (internal/workflow/stage.go) — 1 for a
	// node's first visit, 2+ for a legitimate revisit (force-back, an
	// exclusive-gateway back-edge). Threaded back onto the completion
	// signal (stageTransitionWire/stageFailWire) so the interpreter can tell
	// a stale, leftover signal from an earlier visit apart from the current
	// one's own resolution.
	VisitCount         int64
	AssigneeMode       string
	ConnectorType      *string
	ExtrasJSON         json.RawMessage
	DeferredFromTaskID *uuid.UUID
	DueAt              *time.Time
	FollowUpAt         *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        *time.Time
}
