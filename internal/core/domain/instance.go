package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Instance is the dashboard-facing projection of a running/completed workflow
// instance (LLD §4.2) — Temporal's own execution state is authoritative,
// this row is a query-optimized mirror of it.
type Instance struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	WorkflowID         uuid.UUID
	WorkflowVersionID  uuid.UUID
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
