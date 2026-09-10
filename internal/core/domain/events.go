// Package domain holds Execution Service's outbound event catalogue (LLD
// §6.4): 18 wire-type constants, their payload structs, and one builder
// function per event for sibling tasks (T1.5-T1.8) to call. Shaped after
// iam-user-profile's internal/core/domain/events.go convention — consts,
// payload structs, and the type->message-name mapping colocated in one file.
package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

const EventSource = "workflow-execution-svc"

// Outbound wire-type constants (LLD §6.4). PascalCase, platform-wide
// convention (design/IAM/event-naming-conventions.md) — the SNS `EventType`
// attribute and the envelope `type` field both carry this exact string.
const (
	EventWorkflowInstanceStarted     = "WorkflowInstanceStarted"
	EventWorkflowInstancePaused      = "WorkflowInstancePaused"
	EventWorkflowInstanceResumed     = "WorkflowInstanceResumed"
	EventWorkflowInstanceCancelled   = "WorkflowInstanceCancelled"
	EventWorkflowInstanceTerminated  = "WorkflowInstanceTerminated"
	EventWorkflowInstanceDegraded    = "WorkflowInstanceDegraded"
	EventWorkflowInstanceFailed      = "WorkflowInstanceFailed"
	EventWorkflowInstanceFinished    = "WorkflowInstanceFinished"
	EventWorkflowTaskCreated         = "WorkflowTaskCreated"
	EventWorkflowTaskClaimed         = "WorkflowTaskClaimed"
	EventWorkflowTaskCompleted       = "WorkflowTaskCompleted"
	EventWorkflowTaskDeferred        = "WorkflowTaskDeferred"
	EventWorkflowTaskReassigned      = "WorkflowTaskReassigned"
	EventWorkflowTaskSuperseded      = "WorkflowTaskSuperseded"
	EventWorkflowTaskFailed          = "WorkflowTaskFailed"
	EventWorkflowInstanceForceRouted = "WorkflowInstanceForceRouted"
	EventWorkflowTaskSLAWarning      = "WorkflowTaskSlaWarning"
	EventWorkflowTaskSLABreached     = "WorkflowTaskSlaBreached"
)

// Initiator values for workflow.instance.paused/.resumed (LLD §6.4 table).
const (
	InitiatorAdmin            = "admin"
	InitiatorTenantState      = "tenant_state"
	InitiatorSafetyNet        = "safety_net" // paused only
	InitiatorOOO              = "ooo"
	InitiatorDegradedRecovery = "degraded_recovery" // resumed only
)

// Initiator values for workflow.instance.terminated (LLD §6.4).
const (
	TerminatedInitiatorAdmin       = "admin"
	TerminatedInitiatorTenantState = "tenant_state"
)

// Initiator values for workflow.task.reassigned.
const (
	ReassignInitiatorAdmin      = "admin"
	ReassignInitiatorOverride   = "override"
	ReassignInitiatorDelegation = "delegation"
)

// Direction values for workflow.instance.force-routed.
const (
	ForceRouteDirectionForward = "forward"
	ForceRouteDirectionBack    = "back"
)

// CommonCore is embedded in every one of the 18 outbound event payloads
// (LLD §6.4's "common payload core, all events"). Embedding promotes these
// fields to the top level of the marshaled JSON, so every payload stays a
// flat object even though the Go struct is composed.
type CommonCore struct {
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
	BusinessKey        string    `json:"business_key"`
	WorkflowVersionID  uuid.UUID `json:"workflow_version_id"`
}

// TaskScopedCore is additionally embedded in the 9 workflow.task.* events
// (LLD §6.4's "task-scoped events add" rule). AssigneeUserIDs carries raw
// user IDs only - the Dashboard Stream Gateway owns all display-name
// enrichment (LLD §6.4 Dashboard Stream Gateway payload rule).
type TaskScopedCore struct {
	TaskID          uuid.UUID   `json:"task_id"`
	NodeKey         string      `json:"node_key"`
	DepartmentID    uuid.UUID   `json:"department_id"`
	AssigneeUserIDs []uuid.UUID `json:"assignee_user_ids"`
}

type FailedBranch struct {
	DepartmentID uuid.UUID `json:"department_id"`
	LastNodeKey  string    `json:"last_node_key"`
}

// MessageName maps a wire event-type constant to its
// api/asyncapi.yaml components.messages key. Doc/test cross-referencing
// only - it has no runtime role in Glue schema resolution, which uses the
// wire type string directly (see internal/adapter/outbound/glue.Codec). Now
// that the wire type constants are themselves PascalCase, the AsyncAPI
// message key is identical to the wire type — this is an identity function.
func MessageName(eventType string) string {
	return eventType
}

type WorkflowInstanceStartedPayload struct {
	CommonCore
	StartedByUserID uuid.UUID `json:"started_by_user_id"`
}

func NewWorkflowInstanceStartedPayload(core CommonCore, startedByUserID uuid.UUID) WorkflowInstanceStartedPayload {
	return WorkflowInstanceStartedPayload{CommonCore: core, StartedByUserID: startedByUserID}
}

type WorkflowInstancePausedPayload struct {
	CommonCore
	StartedByUserID uuid.UUID  `json:"started_by_user_id"`
	Initiator       string     `json:"initiator"`
	ActorUserID     *uuid.UUID `json:"actor_user_id,omitempty"`
}

func NewWorkflowInstancePausedPayload(core CommonCore, startedByUserID uuid.UUID, initiator string, actorUserID *uuid.UUID) WorkflowInstancePausedPayload {
	return WorkflowInstancePausedPayload{CommonCore: core, StartedByUserID: startedByUserID, Initiator: initiator, ActorUserID: actorUserID}
}

type WorkflowInstanceResumedPayload struct {
	CommonCore
	StartedByUserID uuid.UUID  `json:"started_by_user_id"`
	Initiator       string     `json:"initiator"`
	ActorUserID     *uuid.UUID `json:"actor_user_id,omitempty"`
}

func NewWorkflowInstanceResumedPayload(core CommonCore, startedByUserID uuid.UUID, initiator string, actorUserID *uuid.UUID) WorkflowInstanceResumedPayload {
	return WorkflowInstanceResumedPayload{CommonCore: core, StartedByUserID: startedByUserID, Initiator: initiator, ActorUserID: actorUserID}
}

type WorkflowInstanceCancelledPayload struct {
	CommonCore
	StartedByUserID uuid.UUID `json:"started_by_user_id"`
	ActorUserID     uuid.UUID `json:"actor_user_id"`
	Reason          *string   `json:"reason,omitempty"`
}

func NewWorkflowInstanceCancelledPayload(core CommonCore, startedByUserID, actorUserID uuid.UUID, reason *string) WorkflowInstanceCancelledPayload {
	return WorkflowInstanceCancelledPayload{CommonCore: core, StartedByUserID: startedByUserID, ActorUserID: actorUserID, Reason: reason}
}

type WorkflowInstanceTerminatedPayload struct {
	CommonCore
	StartedByUserID uuid.UUID  `json:"started_by_user_id"`
	Initiator       string     `json:"initiator"`
	ActorUserID     *uuid.UUID `json:"actor_user_id,omitempty"`
}

func NewWorkflowInstanceTerminatedPayload(core CommonCore, startedByUserID uuid.UUID, initiator string, actorUserID *uuid.UUID) WorkflowInstanceTerminatedPayload {
	return WorkflowInstanceTerminatedPayload{CommonCore: core, StartedByUserID: startedByUserID, Initiator: initiator, ActorUserID: actorUserID}
}

type WorkflowInstanceDegradedPayload struct {
	CommonCore
	FailedBranches []FailedBranch `json:"failed_branches"`
}

func NewWorkflowInstanceDegradedPayload(core CommonCore, failedBranches []FailedBranch) WorkflowInstanceDegradedPayload {
	return WorkflowInstanceDegradedPayload{CommonCore: core, FailedBranches: failedBranches}
}

type WorkflowInstanceFailedPayload struct {
	CommonCore
	ErrorClass string `json:"error_class"`
}

func NewWorkflowInstanceFailedPayload(core CommonCore, errorClass string) WorkflowInstanceFailedPayload {
	return WorkflowInstanceFailedPayload{CommonCore: core, ErrorClass: errorClass}
}

type WorkflowInstanceFinishedPayload struct {
	CommonCore
	StartedByUserID uuid.UUID `json:"started_by_user_id"`
	CompletedAt     time.Time `json:"completed_at"`
}

func NewWorkflowInstanceFinishedPayload(core CommonCore, startedByUserID uuid.UUID, completedAt time.Time) WorkflowInstanceFinishedPayload {
	return WorkflowInstanceFinishedPayload{CommonCore: core, StartedByUserID: startedByUserID, CompletedAt: completedAt}
}

// workflow.instance.force-routed is instance-scoped despite the "instance"
// naming already implying it - it does NOT get TaskScopedCore.
type WorkflowInstanceForceRoutedPayload struct {
	CommonCore
	ActorUserID  uuid.UUID `json:"actor_user_id"`
	FromNodeKeys []string  `json:"from_node_keys"`
	ToNodeKey    string    `json:"to_node_key"`
	Direction    string    `json:"direction"`
}

func NewWorkflowInstanceForceRoutedPayload(core CommonCore, actorUserID uuid.UUID, fromNodeKeys []string, toNodeKey, direction string) WorkflowInstanceForceRoutedPayload {
	return WorkflowInstanceForceRoutedPayload{CommonCore: core, ActorUserID: actorUserID, FromNodeKeys: fromNodeKeys, ToNodeKey: toNodeKey, Direction: direction}
}

type WorkflowTaskCreatedPayload struct {
	CommonCore
	TaskScopedCore
	DueAt          *time.Time     `json:"due_at,omitempty"`
	FollowUpAt     *time.Time     `json:"follow_up_at,omitempty"`
	StageType      string         `json:"stage_type"`
	ConnectorType  *string        `json:"connector_type,omitempty"`
	ResolvedInputs map[string]any `json:"resolved_inputs,omitempty"`
	OutputMapping  []dsl.IOVar    `json:"output_mapping,omitempty"`
}

func NewWorkflowTaskCreatedPayload(
	core CommonCore, task TaskScopedCore, stageType string, dueAt, followUpAt *time.Time,
	connectorType *string, resolvedInputs map[string]any, outputMapping []dsl.IOVar,
) WorkflowTaskCreatedPayload {
	return WorkflowTaskCreatedPayload{
		CommonCore: core, TaskScopedCore: task, StageType: stageType, DueAt: dueAt, FollowUpAt: followUpAt,
		ConnectorType: connectorType, ResolvedInputs: resolvedInputs, OutputMapping: outputMapping,
	}
}

type WorkflowTaskClaimedPayload struct {
	CommonCore
	TaskScopedCore
	ClaimedByUserID uuid.UUID `json:"claimed_by_user_id"`
}

func NewWorkflowTaskClaimedPayload(core CommonCore, task TaskScopedCore, claimedByUserID uuid.UUID) WorkflowTaskClaimedPayload {
	return WorkflowTaskClaimedPayload{CommonCore: core, TaskScopedCore: task, ClaimedByUserID: claimedByUserID}
}

type WorkflowTaskCompletedPayload struct {
	CommonCore
	TaskScopedCore
	CompletedByUserID uuid.UUID `json:"completed_by_user_id"`
}

func NewWorkflowTaskCompletedPayload(core CommonCore, task TaskScopedCore, completedByUserID uuid.UUID) WorkflowTaskCompletedPayload {
	return WorkflowTaskCompletedPayload{CommonCore: core, TaskScopedCore: task, CompletedByUserID: completedByUserID}
}

type WorkflowTaskDeferredPayload struct {
	CommonCore
	TaskScopedCore
	DeferredToNodeKey string     `json:"deferred_to_node_key"`
	Reason            *string    `json:"reason,omitempty"`
	DueAt             *time.Time `json:"due_at,omitempty"`
}

func NewWorkflowTaskDeferredPayload(core CommonCore, task TaskScopedCore, deferredToNodeKey string, reason *string, dueAt *time.Time) WorkflowTaskDeferredPayload {
	return WorkflowTaskDeferredPayload{CommonCore: core, TaskScopedCore: task, DeferredToNodeKey: deferredToNodeKey, Reason: reason, DueAt: dueAt}
}

type WorkflowTaskReassignedPayload struct {
	CommonCore
	TaskScopedCore
	OldUserID    uuid.UUID  `json:"old_user_id"`
	NewUserID    uuid.UUID  `json:"new_user_id"`
	Initiator    string     `json:"initiator"`
	DelegationID *uuid.UUID `json:"delegation_id,omitempty"`
}

func NewWorkflowTaskReassignedPayload(core CommonCore, task TaskScopedCore, oldUserID, newUserID uuid.UUID, initiator string, delegationID *uuid.UUID) WorkflowTaskReassignedPayload {
	return WorkflowTaskReassignedPayload{CommonCore: core, TaskScopedCore: task, OldUserID: oldUserID, NewUserID: newUserID, Initiator: initiator, DelegationID: delegationID}
}

type WorkflowTaskSupersededPayload struct {
	CommonCore
	TaskScopedCore
	ActorUserID uuid.UUID `json:"actor_user_id"`
}

func NewWorkflowTaskSupersededPayload(core CommonCore, task TaskScopedCore, actorUserID uuid.UUID) WorkflowTaskSupersededPayload {
	return WorkflowTaskSupersededPayload{CommonCore: core, TaskScopedCore: task, ActorUserID: actorUserID}
}

type WorkflowTaskFailedPayload struct {
	CommonCore
	TaskScopedCore
	CascadeSource string `json:"cascade_source"`
}

func NewWorkflowTaskFailedPayload(core CommonCore, task TaskScopedCore, cascadeSource string) WorkflowTaskFailedPayload {
	return WorkflowTaskFailedPayload{CommonCore: core, TaskScopedCore: task, CascadeSource: cascadeSource}
}

type WorkflowTaskSLAWarningPayload struct {
	CommonCore
	TaskScopedCore
	FollowUpAt time.Time `json:"follow_up_at"`
}

func NewWorkflowTaskSLAWarningPayload(core CommonCore, task TaskScopedCore, followUpAt time.Time) WorkflowTaskSLAWarningPayload {
	return WorkflowTaskSLAWarningPayload{CommonCore: core, TaskScopedCore: task, FollowUpAt: followUpAt}
}

type WorkflowTaskSLABreachedPayload struct {
	CommonCore
	TaskScopedCore
	DueAt time.Time `json:"due_at"`
}

func NewWorkflowTaskSLABreachedPayload(core CommonCore, task TaskScopedCore, dueAt time.Time) WorkflowTaskSLABreachedPayload {
	return WorkflowTaskSLABreachedPayload{CommonCore: core, TaskScopedCore: task, DueAt: dueAt}
}
