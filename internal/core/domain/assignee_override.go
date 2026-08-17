package domain

import (
	"time"

	"github.com/google/uuid"
)

// AssigneeOverride is an assignee_overrides row (LLD §4.12): insert-only —
// no record_version column of its own, since a node-override target's
// version-checked resource is the workflow_task at that node, not this
// audit-trail row (see port.AssigneeOverride's own doc comment).
type AssigneeOverride struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	WorkflowInstanceID uuid.UUID
	NodeKey            string
	PreviousUserID     uuid.UUID
	NewUserID          uuid.UUID
	Reason             string
	ActorUserID        uuid.UUID
	CreatedAt          time.Time
}
