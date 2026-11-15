package port

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
)

// SignalInstanceTaskVacated tells a running workflow that a task lost its last
// active assignee (execution LLD §5.8).
const SignalInstanceTaskVacated = "instance-task-vacated"

// WorkflowSignalEventType is the outbox record type of a pending workflow
// signal. The relay hands these to Temporal and never publishes them to the
// event bus.
const WorkflowSignalEventType = "WorkflowSignalPending"

// WorkflowSignal is a signal recorded for a workflow in the same transaction
// as the change it announces.
type WorkflowSignal struct {
	TenantID           uuid.UUID
	InstanceID         uuid.UUID
	TemporalWorkflowID string
	Name               string
	Payload            json.RawMessage
}

// WorkflowSignalMessage is the data an outbox record of type
// WorkflowSignalEventType carries. It deliberately has no workflow_instance_id
// or task_id key, so the instance event listing and the task-keyed outbox
// lookups never see these records.
type WorkflowSignalMessage struct {
	TargetInstanceID   string          `json:"target_instance_id"`
	TemporalWorkflowID string          `json:"temporal_workflow_id"`
	SignalName         string          `json:"signal_name"`
	SignalPayload      json.RawMessage `json:"signal_payload"`
}

// WorkflowSignalOutbox records workflow signals for the outbox relay to deliver
// with retries. Delivery is at least once and not ordered: a signal that fails
// is retried later than one recorded after it, so a signal that depends on
// order carries the time it was recorded.
type WorkflowSignalOutbox interface {
	// EnqueueSignal must run inside a transaction opened with Transactor.RunInTx.
	EnqueueSignal(ctx context.Context, sig WorkflowSignal) error
}
