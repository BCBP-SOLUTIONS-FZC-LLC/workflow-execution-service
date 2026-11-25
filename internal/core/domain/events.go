package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func OutboundEventTypes() []string {
	return []string{
		EventWorkflowInstanceStarted, EventWorkflowInstancePaused, EventWorkflowInstanceResumed, EventWorkflowInstanceCancelled,
		EventWorkflowInstanceTerminated, EventWorkflowInstanceDegraded, EventWorkflowInstanceFailed, EventWorkflowInstanceFinished,
		EventWorkflowInstanceForceRouted, EventWorkflowTaskCreated, EventWorkflowTaskClaimed, EventWorkflowTaskCompleted,
		EventWorkflowTaskDeferred, EventWorkflowTaskReassigned, EventWorkflowTaskSuperseded, EventWorkflowTaskFailed,
		EventWorkflowTaskSLAWarning, EventWorkflowTaskSLABreached,
	}
}

const EventSource = "workflow-execution-svc"

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

const (
	InitiatorAdmin            = "admin"
	InitiatorTenantState      = "tenant_state"
	InitiatorSafetyNet        = "safety_net"
	InitiatorOOO              = "ooo"
	InitiatorDegradedRecovery = "degraded_recovery"
)

const CascadeSourceInstanceTerminate = "instance_terminate"

const (
	TerminatedInitiatorAdmin       = "admin"
	TerminatedInitiatorTenantState = "tenant_state"
)

const (
	ReassignInitiatorAdmin      = "admin"
	ReassignInitiatorOverride   = "override"
	ReassignInitiatorDelegation = "delegation"
)

const (
	ForceRouteDirectionForward = "forward"
	ForceRouteDirectionBack    = "back"
)

type CommonCore struct {
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
	SubjectID          string    `json:"subject_id"`
	BusinessKey        string    `json:"business_key"`
	WorkflowVersionID  uuid.UUID `json:"workflow_version_id"`
}

type TaskScopedCore struct {
	TaskID          uuid.UUID   `json:"task_id"`
	NodeKey         string      `json:"node_key"`
	DepartmentID    uuid.UUID   `json:"department_id"`
	AssigneeUserIDs []uuid.UUID `json:"assignee_user_ids"`
}

func (t TaskScopedCore) withNonNilAssignees() TaskScopedCore {
	if t.AssigneeUserIDs == nil {
		t.AssigneeUserIDs = []uuid.UUID{}
	}
	return t
}

type RecipientKind string

const (
	RecipientUser       RecipientKind = "user"
	RecipientDepartment RecipientKind = "department"
)

type Recipient struct {
	Kind RecipientKind `json:"kind"`
	ID   string        `json:"id"`
}

func recipientsFor(users []uuid.UUID, departmentID uuid.UUID) []Recipient {
	out := make([]Recipient, 0, len(users)+1)
	for _, u := range users {
		if u != uuid.Nil {
			out = append(out, Recipient{Kind: RecipientUser, ID: u.String()})
		}
	}
	if departmentID != uuid.Nil {
		out = append(out, Recipient{Kind: RecipientDepartment, ID: departmentID.String()})
	}
	return out
}

func (t TaskScopedCore) recipients() []Recipient {
	return recipientsFor(t.AssigneeUserIDs, t.DepartmentID)
}

func starterRecipients(startedByUserID uuid.UUID) []Recipient {
	return recipientsFor([]uuid.UUID{startedByUserID}, uuid.Nil)
}

type FailedBranch struct {
	DepartmentID uuid.UUID `json:"department_id"`
	LastNodeKey  string    `json:"last_node_key"`
}

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
	Recipients      []Recipient `json:"recipients"`
	StartedByUserID uuid.UUID   `json:"started_by_user_id"`
	Initiator       string      `json:"initiator"`
	ActorUserID     *uuid.UUID  `json:"actor_user_id,omitempty"`
}

func NewWorkflowInstancePausedPayload(core CommonCore, startedByUserID uuid.UUID, initiator string, actorUserID *uuid.UUID) WorkflowInstancePausedPayload {
	return WorkflowInstancePausedPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, Initiator: initiator, ActorUserID: actorUserID}
}

type WorkflowInstanceResumedPayload struct {
	CommonCore
	Recipients      []Recipient `json:"recipients"`
	StartedByUserID uuid.UUID   `json:"started_by_user_id"`
	Initiator       string      `json:"initiator"`
	ActorUserID     *uuid.UUID  `json:"actor_user_id,omitempty"`
}

func NewWorkflowInstanceResumedPayload(core CommonCore, startedByUserID uuid.UUID, initiator string, actorUserID *uuid.UUID) WorkflowInstanceResumedPayload {
	return WorkflowInstanceResumedPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, Initiator: initiator, ActorUserID: actorUserID}
}

type WorkflowInstanceCancelledPayload struct {
	CommonCore
	Recipients      []Recipient `json:"recipients"`
	StartedByUserID uuid.UUID   `json:"started_by_user_id"`
	ActorUserID     uuid.UUID   `json:"actor_user_id"`
	Reason          *string     `json:"reason,omitempty"`
}

func NewWorkflowInstanceCancelledPayload(core CommonCore, startedByUserID, actorUserID uuid.UUID, reason *string) WorkflowInstanceCancelledPayload {
	return WorkflowInstanceCancelledPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, ActorUserID: actorUserID, Reason: reason}
}

type WorkflowInstanceTerminatedPayload struct {
	CommonCore
	Recipients      []Recipient `json:"recipients"`
	StartedByUserID uuid.UUID   `json:"started_by_user_id"`
	Initiator       string      `json:"initiator"`
	ActorUserID     *uuid.UUID  `json:"actor_user_id,omitempty"`
	Reason          *string     `json:"reason,omitempty"`
}

func NewWorkflowInstanceTerminatedPayload(core CommonCore, startedByUserID uuid.UUID, initiator string, actorUserID *uuid.UUID, reason *string) WorkflowInstanceTerminatedPayload {
	return WorkflowInstanceTerminatedPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, Initiator: initiator, ActorUserID: actorUserID, Reason: reason}
}

type WorkflowInstanceDegradedPayload struct {
	CommonCore
	Recipients      []Recipient    `json:"recipients"`
	StartedByUserID uuid.UUID      `json:"started_by_user_id"`
	FailedBranches  []FailedBranch `json:"failed_branches"`
}

func NewWorkflowInstanceDegradedPayload(core CommonCore, startedByUserID uuid.UUID, failedBranches []FailedBranch) WorkflowInstanceDegradedPayload {
	if failedBranches == nil {
		failedBranches = []FailedBranch{}
	}
	return WorkflowInstanceDegradedPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, FailedBranches: failedBranches}
}

type WorkflowInstanceFailedPayload struct {
	CommonCore
	Recipients      []Recipient `json:"recipients"`
	StartedByUserID uuid.UUID   `json:"started_by_user_id"`
	ErrorClass      string      `json:"error_class"`
}

func NewWorkflowInstanceFailedPayload(core CommonCore, startedByUserID uuid.UUID, errorClass string) WorkflowInstanceFailedPayload {
	return WorkflowInstanceFailedPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, ErrorClass: errorClass}
}

type WorkflowInstanceFinishedPayload struct {
	CommonCore
	Recipients      []Recipient `json:"recipients"`
	StartedByUserID uuid.UUID   `json:"started_by_user_id"`
	CompletedAt     time.Time   `json:"completed_at"`
}

func NewWorkflowInstanceFinishedPayload(core CommonCore, startedByUserID uuid.UUID, completedAt time.Time) WorkflowInstanceFinishedPayload {
	return WorkflowInstanceFinishedPayload{CommonCore: core, Recipients: starterRecipients(startedByUserID), StartedByUserID: startedByUserID, CompletedAt: completedAt}
}

type WorkflowInstanceForceRoutedPayload struct {
	CommonCore
	ActorUserID  uuid.UUID `json:"actor_user_id"`
	FromNodeKeys []string  `json:"from_node_keys"`
	ToNodeKey    string    `json:"to_node_key"`
	Direction    string    `json:"direction"`
}

func NewWorkflowInstanceForceRoutedPayload(core CommonCore, actorUserID uuid.UUID, fromNodeKeys []string, toNodeKey, direction string) WorkflowInstanceForceRoutedPayload {
	if fromNodeKeys == nil {
		fromNodeKeys = []string{}
	}
	return WorkflowInstanceForceRoutedPayload{CommonCore: core, ActorUserID: actorUserID, FromNodeKeys: fromNodeKeys, ToNodeKey: toNodeKey, Direction: direction}
}

type WorkflowTaskCreatedPayload struct {
	CommonCore
	TaskScopedCore
	Recipients     []Recipient    `json:"recipients"`
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
		CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), StageType: stageType, DueAt: dueAt, FollowUpAt: followUpAt,
		ConnectorType: connectorType, ResolvedInputs: resolvedInputs, OutputMapping: outputMapping,
	}
}

type WorkflowTaskClaimedPayload struct {
	CommonCore
	TaskScopedCore
	Recipients      []Recipient `json:"recipients"`
	ClaimedByUserID uuid.UUID   `json:"claimed_by_user_id"`
}

func NewWorkflowTaskClaimedPayload(core CommonCore, task TaskScopedCore, claimedByUserID uuid.UUID) WorkflowTaskClaimedPayload {
	return WorkflowTaskClaimedPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), ClaimedByUserID: claimedByUserID}
}

type WorkflowTaskCompletedPayload struct {
	CommonCore
	TaskScopedCore
	Recipients        []Recipient `json:"recipients"`
	CompletedByUserID uuid.UUID   `json:"completed_by_user_id"`
}

func NewWorkflowTaskCompletedPayload(core CommonCore, task TaskScopedCore, completedByUserID uuid.UUID) WorkflowTaskCompletedPayload {
	return WorkflowTaskCompletedPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), CompletedByUserID: completedByUserID}
}

type WorkflowTaskDeferredPayload struct {
	CommonCore
	TaskScopedCore
	Recipients        []Recipient `json:"recipients"`
	DeferredToNodeKey string      `json:"deferred_to_node_key"`
	Reason            *string     `json:"reason,omitempty"`
	DueAt             *time.Time  `json:"due_at,omitempty"`
}

func NewWorkflowTaskDeferredPayload(core CommonCore, task TaskScopedCore, deferredToNodeKey string, reason *string, dueAt *time.Time) WorkflowTaskDeferredPayload {
	return WorkflowTaskDeferredPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), DeferredToNodeKey: deferredToNodeKey, Reason: reason, DueAt: dueAt}
}

type WorkflowTaskReassignedPayload struct {
	CommonCore
	TaskScopedCore
	Recipients   []Recipient `json:"recipients"`
	OldUserID    *uuid.UUID  `json:"old_user_id,omitempty"`
	NewUserID    uuid.UUID   `json:"new_user_id"`
	Initiator    string      `json:"initiator"`
	DelegationID *uuid.UUID  `json:"delegation_id,omitempty"`
}

func NewWorkflowTaskReassignedPayload(core CommonCore, task TaskScopedCore, oldUserID *uuid.UUID, newUserID uuid.UUID, initiator string, delegationID *uuid.UUID) WorkflowTaskReassignedPayload {
	users := []uuid.UUID{newUserID}
	if oldUserID != nil {
		users = []uuid.UUID{*oldUserID, newUserID}
	}
	return WorkflowTaskReassignedPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: recipientsFor(users, task.DepartmentID), OldUserID: oldUserID, NewUserID: newUserID, Initiator: initiator, DelegationID: delegationID}
}

// OptionalUserID is the pointer form of a user id the caller holds as the all-zero id when nobody is meant.
func OptionalUserID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

type WorkflowTaskSupersededPayload struct {
	CommonCore
	TaskScopedCore
	Recipients  []Recipient `json:"recipients"`
	ActorUserID *uuid.UUID  `json:"actor_user_id,omitempty"`
}

// NewWorkflowTaskSupersededPayload leaves the actor out when actorUserID is the all-zero id, as when the system superseded the task.
func NewWorkflowTaskSupersededPayload(core CommonCore, task TaskScopedCore, actorUserID uuid.UUID) WorkflowTaskSupersededPayload {
	return WorkflowTaskSupersededPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), ActorUserID: OptionalUserID(actorUserID)}
}

type WorkflowTaskFailedPayload struct {
	CommonCore
	TaskScopedCore
	Recipients    []Recipient `json:"recipients"`
	CascadeSource string      `json:"cascade_source"`
	ErrorClass    string      `json:"error_class,omitempty"`
}

func NewWorkflowTaskFailedPayload(core CommonCore, task TaskScopedCore, cascadeSource, errorClass string) WorkflowTaskFailedPayload {
	return WorkflowTaskFailedPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), CascadeSource: cascadeSource, ErrorClass: errorClass}
}

type WorkflowTaskSLAWarningPayload struct {
	CommonCore
	TaskScopedCore
	Recipients []Recipient `json:"recipients"`
	FollowUpAt time.Time   `json:"follow_up_at"`
}

func NewWorkflowTaskSLAWarningPayload(core CommonCore, task TaskScopedCore, followUpAt time.Time) WorkflowTaskSLAWarningPayload {
	return WorkflowTaskSLAWarningPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), FollowUpAt: followUpAt}
}

type WorkflowTaskSLABreachedPayload struct {
	CommonCore
	TaskScopedCore
	Recipients []Recipient `json:"recipients"`
	DueAt      time.Time   `json:"due_at"`
}

func NewWorkflowTaskSLABreachedPayload(core CommonCore, task TaskScopedCore, dueAt time.Time) WorkflowTaskSLABreachedPayload {
	return WorkflowTaskSLABreachedPayload{CommonCore: core, TaskScopedCore: task.withNonNilAssignees(), Recipients: task.recipients(), DueAt: dueAt}
}

const (
	TenantStatusTrial        = "trial"
	TenantStatusActive       = "active"
	TenantStatusPastDue      = "past_due"
	TenantStatusCancelled    = "cancelled"
	TenantStatusSuspended    = "suspended"
	TenantStatusTrialExpired = "trial_expired"
	TenantStatusOffboarded   = "offboarded"
)

func IsKnownTenantStatus(status string) bool {
	switch status {
	case TenantStatusTrial, TenantStatusActive, TenantStatusPastDue, TenantStatusCancelled,
		TenantStatusSuspended, TenantStatusTrialExpired, TenantStatusOffboarded:
		return true
	default:
		return false
	}
}
