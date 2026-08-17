package port

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type InstanceStatus string

const (
	InstanceStatusRunning    InstanceStatus = "RUNNING"
	InstanceStatusPaused     InstanceStatus = "PAUSED"
	InstanceStatusCompleted  InstanceStatus = "COMPLETED"
	InstanceStatusTerminated InstanceStatus = "TERMINATED"
	InstanceStatusFailed     InstanceStatus = "FAILED"
	InstanceStatusDegraded   InstanceStatus = "DEGRADED"
)

// Instance mirrors feat/persistence-layer's real domain.Instance field-for-
// field (verified directly against that unmerged branch) so a future
// type-swap, once that branch merges, is a cheap rename rather than a
// redesign.
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

type InstanceFilter struct {
	Status            *InstanceStatus
	WorkflowVersionID *uuid.UUID
	StartedAfter      *time.Time
	StartedBefore     *time.Time
}

// StartInstanceInput is typed at the port boundary: OverrideMap is parsed to
// map[string]uuid.UUID by request binding, so a malformed UUID value is a
// 400 before this ever reaches the service (LLD §5.2).
type StartInstanceInput struct {
	TenantID          uuid.UUID
	WorkflowVersionID uuid.UUID
	BusinessKey       string
	ContextJSON       json.RawMessage
	OverrideMap       map[string]uuid.UUID
	StartedByUserID   uuid.UUID
}

type WorkflowEventType string

const (
	EventInstanceStarted    WorkflowEventType = "INSTANCE_STARTED"
	EventInstancePaused     WorkflowEventType = "INSTANCE_PAUSED"
	EventInstanceResumed    WorkflowEventType = "INSTANCE_RESUMED"
	EventInstanceCompleted  WorkflowEventType = "INSTANCE_COMPLETED"
	EventInstanceTerminated WorkflowEventType = "INSTANCE_TERMINATED"
	EventInstanceFailed     WorkflowEventType = "INSTANCE_FAILED"
	EventTaskCreated        WorkflowEventType = "TASK_CREATED"
	EventTaskAssigned       WorkflowEventType = "TASK_ASSIGNED"
	EventTaskStarted        WorkflowEventType = "TASK_STARTED"
	EventTaskCompleted      WorkflowEventType = "TASK_COMPLETED"
	EventTaskDeferred       WorkflowEventType = "TASK_DEFERRED"
	// EventTaskRejected is reserved for future explicit rejection workflows;
	// deferral emits EventTaskDeferred instead (LLD §4.2/OpenAPI note).
	EventTaskRejected   WorkflowEventType = "TASK_REJECTED"
	EventTaskReassigned WorkflowEventType = "TASK_REASSIGNED"
	// EventCommentAdded/EventResourceLinked are vestigial — the underlying
	// tables were removed from scope (LLD §5.2) — kept for enum parity with
	// the OpenAPI spec's WorkflowEventType schema; never emitted.
	EventCommentAdded   WorkflowEventType = "COMMENT_ADDED"
	EventResourceLinked WorkflowEventType = "RESOURCE_LINKED"

	// The 8 constants below complete this enum against the real 18-event
	// domain.Event* outbound catalogue (internal/core/domain/events.go) —
	// the original enum above predates that full catalogue and only covered
	// a subset. Added when ListEvents' domain-event-type -> WorkflowEventType
	// mapping (internal/core/service/mapping.go) was actually implemented,
	// so every real wire type this schema emits has somewhere to map to.
	EventInstanceCancelled   WorkflowEventType = "INSTANCE_CANCELLED"
	EventInstanceDegraded    WorkflowEventType = "INSTANCE_DEGRADED"
	EventInstanceForceRouted WorkflowEventType = "INSTANCE_FORCE_ROUTED"
	EventTaskClaimed         WorkflowEventType = "TASK_CLAIMED"
	EventTaskSuperseded      WorkflowEventType = "TASK_SUPERSEDED"
	EventTaskFailed          WorkflowEventType = "TASK_FAILED"
	EventTaskSLAWarning      WorkflowEventType = "TASK_SLA_WARNING"
	EventTaskSLABreached     WorkflowEventType = "TASK_SLA_BREACHED"
)

// WorkflowEvent is a single outbox_events row projected for the activity-log
// endpoint (LLD §4.5/§4.9): there is no dedicated workflow_event table, so
// this shape is read from payload -> 'data' (never a flat payload ->> field
// — rev 1.17's fix, since outbox.Enqueue marshals the whole envelope), with
// created_at filling the occurred_at role.
type WorkflowEvent struct {
	ID                 uuid.UUID
	WorkflowInstanceID uuid.UUID
	TaskID             *uuid.UUID
	TenantID           uuid.UUID
	EventType          WorkflowEventType
	ActorUserID        *uuid.UUID
	NodeKey            *string
	PayloadJSON        json.RawMessage
	OccurredAt         time.Time
}

// InstanceService is the port internal/adapter/inbound/http/handler codes
// against. The six lifecycle-signal methods return error only, not
// (*Instance, error): their documented 202 response is the minimal
// SignalAccepted{message} body (LLD §5.2/§5.10), not the mutated resource.
// Every mutating method performs its own synchronous record_version/state
// pre-check before forwarding a signal (LLD §5.10); Terminate has no
// record_version parameter at all — it's a direct TerminateWorkflow call,
// not signal-validated (LLD §3.1).
type InstanceService interface {
	Start(ctx context.Context, in StartInstanceInput) (*Instance, error)
	List(ctx context.Context, tenantID uuid.UUID, scope ReadScope, filter InstanceFilter, page Page) (PageResult[*Instance], error)
	Get(ctx context.Context, tenantID, instanceID uuid.UUID, scope ReadScope) (*Instance, []*Task, error)

	Pause(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error
	Resume(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error
	Cancel(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error
	Terminate(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error
	ForceForward(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error
	ForceBack(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error

	// ListEvents applies the same intra-tenant visibility check as Get (LLD
	// §9.2) — the events endpoint is a sub-resource of the same instance.
	ListEvents(ctx context.Context, tenantID, instanceID uuid.UUID, scope ReadScope, page Page) (PageResult[*WorkflowEvent], error)
}
