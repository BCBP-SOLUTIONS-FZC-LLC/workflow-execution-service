package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Instance struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	WorkflowID         uuid.UUID
	WorkflowVersionID  uuid.UUID
	SubjectID          string
	BusinessKey        string
	TemporalWorkflowID string
	TemporalRunID      string
	Status             InstanceStatus
	CurrentNodeKeys    []string
	SavedNodeKeys      []string
	ContextJSON        json.RawMessage
	OverrideMap        json.RawMessage
	TaskQueue          string
	StartedByUserID    uuid.UUID
	StartedAt          *time.Time
	CompletedAt        *time.Time
	RecordVersion      int64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
