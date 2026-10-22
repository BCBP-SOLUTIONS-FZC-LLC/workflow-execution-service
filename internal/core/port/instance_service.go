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

type InstanceFilter struct {
	Status            *InstanceStatus
	SubjectID         *string
	WorkflowVersionID *uuid.UUID
	StartedAfter      *time.Time
	StartedBefore     *time.Time
}

type StartInstanceInput struct {
	TenantID          uuid.UUID
	WorkflowVersionID uuid.UUID
	SubjectID         string
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
	EventTaskRejected       WorkflowEventType = "TASK_REJECTED"
	EventTaskReassigned     WorkflowEventType = "TASK_REASSIGNED"
	EventCommentAdded       WorkflowEventType = "COMMENT_ADDED"
	EventResourceLinked     WorkflowEventType = "RESOURCE_LINKED"

	EventInstanceCancelled   WorkflowEventType = "INSTANCE_CANCELLED"
	EventInstanceDegraded    WorkflowEventType = "INSTANCE_DEGRADED"
	EventInstanceForceRouted WorkflowEventType = "INSTANCE_FORCE_ROUTED"
	EventTaskClaimed         WorkflowEventType = "TASK_CLAIMED"
	EventTaskSuperseded      WorkflowEventType = "TASK_SUPERSEDED"
	EventTaskFailed          WorkflowEventType = "TASK_FAILED"
	EventTaskSLAWarning      WorkflowEventType = "TASK_SLA_WARNING"
	EventTaskSLABreached     WorkflowEventType = "TASK_SLA_BREACHED"
)

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

type InstanceService interface {
	Start(ctx context.Context, in StartInstanceInput) (*Instance, error)
	List(ctx context.Context, tenantID uuid.UUID, scope ReadScope, filter InstanceFilter, page Page) (PageResult[*Instance], error)
	Get(ctx context.Context, tenantID, instanceID uuid.UUID, scope ReadScope) (*Instance, []*Task, error)

	Pause(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error
	Resume(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error
	Cancel(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error
	Terminate(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error
	ForceForward(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error
	ForceBack(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error

	AnswerOutsideWait(ctx context.Context, tenantID, instanceID uuid.UUID, scope ReadScope, key string, variables map[string]any) error

	ListEvents(ctx context.Context, tenantID, instanceID uuid.UUID, scope ReadScope, page Page) (PageResult[*WorkflowEvent], error)
}
