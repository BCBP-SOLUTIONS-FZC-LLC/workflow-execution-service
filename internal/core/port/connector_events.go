package port

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

type ConnectorEventPublisher interface {
	PublishTaskCreated(ctx context.Context, event ConnectorTaskCreatedEvent) error
}

type ConnectorTaskCreatedEvent struct {
	EventID        uuid.UUID
	TenantID       uuid.UUID
	InstanceID     uuid.UUID
	TaskID         uuid.UUID
	NodeKey        string
	DepartmentID   uuid.UUID
	ConnectorType  string
	ResolvedInputs map[string]any
	OutputMapping  []dsl.IOVar
}
