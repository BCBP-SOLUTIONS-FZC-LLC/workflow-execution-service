package port

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// ConnectorEventPublisher pushes a connector-typed workflow.task.created
// event onto the Valkey Stream cmd/connector-worker consumes (LLD
// workflow_connectors.md §6.5 step 0). Nil-safe at call sites the same way
// port.CacheStore is — an unconfigured Stream in dev just drops these.
type ConnectorEventPublisher interface {
	PublishTaskCreated(ctx context.Context, event ConnectorTaskCreatedEvent) error
}

// ConnectorTaskCreatedEvent is what internal_events.go's own inbound
// handler for workflow.task.created forwards onto the Stream once it
// confirms the task is connector-typed. OutputMapping mirrors the compiled
// stage's IOMapping.Outputs, persisted by CreateTaskActivity — the new
// connector-task completion endpoint applies it before signaling, so this
// event's own shape stays a passthrough of what was already computed at
// task-creation time.
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
