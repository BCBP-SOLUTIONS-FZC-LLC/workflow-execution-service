package domain

import (
	"time"

	"github.com/google/uuid"
)

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
