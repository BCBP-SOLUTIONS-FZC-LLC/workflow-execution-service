package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// toPortInstance mirrors domain.Instance -> port.Instance field-for-field
// (the two are deliberately kept in the same shape, port/instance_service.go's
// own doc comment).
func toPortInstance(inst *domain.Instance) *port.Instance {
	return &port.Instance{
		ID:                 inst.ID,
		TenantID:           inst.TenantID,
		WorkflowID:         inst.WorkflowID,
		WorkflowVersionID:  inst.WorkflowVersionID,
		BusinessKey:        inst.BusinessKey,
		TemporalWorkflowID: inst.TemporalWorkflowID,
		TemporalRunID:      inst.TemporalRunID,
		Status:             port.InstanceStatus(inst.Status),
		CurrentNodeKeys:    inst.CurrentNodeKeys,
		SavedNodeKeys:      inst.SavedNodeKeys,
		ContextJSON:        inst.ContextJSON,
		OverrideMap:        inst.OverrideMap,
		TaskQueue:          inst.TaskQueue,
		StartedByUserID:    inst.StartedByUserID,
		StartedAt:          inst.StartedAt,
		CompletedAt:        inst.CompletedAt,
		RecordVersion:      inst.RecordVersion,
		CreatedAt:          inst.CreatedAt,
		UpdatedAt:          inst.UpdatedAt,
	}
}

// toPortTask maps domain.Task -> port.Task. TaskType and RequiredLevel are
// left zero-valued — neither has a real data source yet (LLD Appendix B's
// department_id-class gap; RequiredLevel specifically has no compiled-plan-
// derived column anywhere in this schema today), matching AssigneeCount's
// own already-documented "zero-valued until built" convention on the port
// struct rather than fabricating a value.
func toPortTask(task *domain.Task) *port.Task {
	return &port.Task{
		ID:                 task.ID,
		TenantID:           task.TenantID,
		WorkflowInstanceID: task.WorkflowInstanceID,
		NodeKey:            task.NodeKey,
		DepartmentID:       task.DepartmentID,
		Status:             port.TaskStatus(task.Status),
		RecordVersion:      task.RecordVersion,
		AssigneeMode:       task.AssigneeMode,
		ConnectorType:      task.ConnectorType,
		ExtrasJSON:         task.ExtrasJSON,
		DeferredFromTaskID: task.DeferredFromTaskID,
		DueAt:              task.DueAt,
		FollowUpAt:         task.FollowUpAt,
		CreatedAt:          task.CreatedAt,
		UpdatedAt:          task.UpdatedAt,
		CompletedAt:        task.CompletedAt,
	}
}

func toPortTaskAssignment(a *domain.TaskAssignment) *port.TaskAssignment {
	return &port.TaskAssignment{
		ID:          a.ID,
		TenantID:    a.TenantID,
		TaskID:      a.TaskID,
		UserID:      a.UserID,
		AssignedBy:  a.AssignedBy,
		Reason:      a.Reason,
		IsLead:      a.IsLead,
		IsActive:    a.IsActive,
		AssignedAt:  a.AssignedAt,
		ClaimedAt:   a.ClaimedAt,
		CompletedAt: a.CompletedAt,
		ResultJSON:  a.ResultJSON,
		VacatedAt:   a.VacatedAt,
		UpdatedAt:   a.UpdatedAt,
	}
}

func toPortActiveUserTask(row port.ActiveUserTaskRow) *port.ActiveUserTask {
	return &port.ActiveUserTask{
		TaskID:             row.TaskID,
		WorkflowInstanceID: row.WorkflowInstanceID,
		NodeKey:            row.NodeKey,
		UserID:             row.UserID,
		DepartmentID:       row.DepartmentID,
		Status:             port.TaskStatus(row.Status),
		RecordVersion:      row.RecordVersion,
	}
}

// toPortAssigneeOverride maps domain.AssigneeOverride -> port.AssigneeOverride.
// taskRecordVersion is grafted in from the target task's own post-bump
// record_version, not read from the override row itself — assignee_overrides
// is insert-only with no record_version column of its own (LLD §4.12); the
// LLD's documented response example shows the *task's* new record_version
// (the value the paired instance-reassign signal also carries), so that's
// what the caller (TaskService.OverrideAssignee) must supply here.
func toPortAssigneeOverride(o *domain.AssigneeOverride, taskRecordVersion int64) *port.AssigneeOverride {
	return &port.AssigneeOverride{
		ID:                 o.ID,
		WorkflowInstanceID: o.WorkflowInstanceID,
		NodeKey:            o.NodeKey,
		PreviousUserID:     o.PreviousUserID,
		NewUserID:          o.NewUserID,
		Reason:             o.Reason,
		ActorUserID:        o.ActorUserID,
		RecordVersion:      taskRecordVersion,
		CreatedAt:          o.CreatedAt,
	}
}

// domainEventTypeToPort maps a domain.Event* wire-type string to its
// WorkflowEventType display code. Covers all 18 wire types
// internal/core/domain/events.go defines; ok is false for anything else
// (a genuinely unrecognized/future type), which the caller should treat as
// skip-and-log for one row, not fail the whole page over.
func domainEventTypeToPort(eventType string) (port.WorkflowEventType, bool) {
	switch eventType {
	case domain.EventWorkflowInstanceStarted:
		return port.EventInstanceStarted, true
	case domain.EventWorkflowInstancePaused:
		return port.EventInstancePaused, true
	case domain.EventWorkflowInstanceResumed:
		return port.EventInstanceResumed, true
	case domain.EventWorkflowInstanceCancelled:
		return port.EventInstanceCancelled, true
	case domain.EventWorkflowInstanceTerminated:
		return port.EventInstanceTerminated, true
	case domain.EventWorkflowInstanceDegraded:
		return port.EventInstanceDegraded, true
	case domain.EventWorkflowInstanceFailed:
		return port.EventInstanceFailed, true
	case domain.EventWorkflowInstanceFinished:
		return port.EventInstanceCompleted, true
	case domain.EventWorkflowTaskCreated:
		return port.EventTaskCreated, true
	case domain.EventWorkflowTaskClaimed:
		return port.EventTaskClaimed, true
	case domain.EventWorkflowTaskCompleted:
		return port.EventTaskCompleted, true
	case domain.EventWorkflowTaskDeferred:
		return port.EventTaskDeferred, true
	case domain.EventWorkflowTaskReassigned:
		return port.EventTaskReassigned, true
	case domain.EventWorkflowTaskSuperseded:
		return port.EventTaskSuperseded, true
	case domain.EventWorkflowTaskFailed:
		return port.EventTaskFailed, true
	case domain.EventWorkflowInstanceForceRouted:
		return port.EventInstanceForceRouted, true
	case domain.EventWorkflowTaskSLAWarning:
		return port.EventTaskSLAWarning, true
	case domain.EventWorkflowTaskSLABreached:
		return port.EventTaskSLABreached, true
	default:
		return "", false
	}
}

// outboxEnvelope is the minimal shape of the stored outbox_events.payload
// column this service needs: the full events.Envelope[json.RawMessage]
// wire shape (platform-events/pkg/events), decoded by hand rather than by
// importing that package's own Envelope type, since only a few of its
// fields matter here.
type outboxEnvelope struct {
	TenantID string          `json:"tenant_id"`
	Actor    string          `json:"actor"`
	Data     json.RawMessage `json:"data"`
}

// taskScopedFields is the subset of TaskScopedCore (domain/events.go) every
// task-scoped event payload carries — absent (zero-valued) on instance-scoped
// payloads, which is fine, since both TaskID and NodeKey are optional on
// port.WorkflowEvent.
type taskScopedFields struct {
	TaskID  uuid.UUID `json:"task_id"`
	NodeKey string    `json:"node_key"`
}

var errUnknownEventType = errors.New("unrecognized outbox event type")

// toWorkflowEvent projects one outbox_events row into the activity-log shape
// (LLD §4.5/§4.9/§4.10 — no dedicated workflow_event table). tenantID and
// instanceID come from the caller (ListEvents is already scoped to one
// (tenant, instance) pair via the query itself), not reparsed from the
// payload. ActorUserID is read from the envelope's own top-level "actor"
// field (platform-events' Envelope.Actor) — set by whichever Activity built
// the envelope — not guessed from the payload's own differently-named actor
// fields (started_by_user_id, claimed_by_user_id, completed_by_user_id, ...),
// which vary per event type and often mean something other than "who acted";
// parse failures there just leave ActorUserID nil rather than fail the whole
// row.
func toWorkflowEvent(rec *domain.OutboxEventRecord, tenantID, instanceID uuid.UUID) (*port.WorkflowEvent, error) {
	eventType, ok := domainEventTypeToPort(rec.EventType)
	if !ok {
		return nil, fmt.Errorf("%w: %q", errUnknownEventType, rec.EventType)
	}

	var envelope outboxEnvelope
	if err := json.Unmarshal(rec.Payload, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal outbox event envelope: %w", err)
	}

	var scoped taskScopedFields
	_ = json.Unmarshal(envelope.Data, &scoped) // best-effort: instance-scoped payloads simply lack these keys

	var taskID *uuid.UUID
	var nodeKey *string
	if scoped.TaskID != uuid.Nil {
		id := scoped.TaskID
		taskID = &id
	}
	if scoped.NodeKey != "" {
		nk := scoped.NodeKey
		nodeKey = &nk
	}

	var actorUserID *uuid.UUID
	if envelope.Actor != "" {
		if id, err := uuid.Parse(envelope.Actor); err == nil {
			actorUserID = &id
		}
	}

	return &port.WorkflowEvent{
		ID:                 rec.ID,
		WorkflowInstanceID: instanceID,
		TaskID:             taskID,
		TenantID:           tenantID,
		EventType:          eventType,
		ActorUserID:        actorUserID,
		NodeKey:            nodeKey,
		PayloadJSON:        envelope.Data,
		OccurredAt:         rec.CreatedAt,
	}, nil
}

// wrapInstanceErr translates a domain-layer sentinel from an
// InstanceRepository call into the port-layer sentinel InstanceService
// methods are documented to return (LLD §5.10). Errors it doesn't recognize
// pass through unchanged.
func wrapInstanceErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return port.ErrInstanceNotFound
	case errors.Is(err, domain.ErrRecordVersionConflict):
		return port.ErrRecordVersionConflict
	case errors.Is(err, domain.ErrDuplicateBusinessKey):
		return port.ErrDuplicateBusinessKey
	default:
		return err
	}
}

// wrapTaskErr is wrapInstanceErr's TaskRepository/TaskAssignmentRepository
// counterpart.
func wrapTaskErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return port.ErrTaskNotFound
	case errors.Is(err, domain.ErrRecordVersionConflict):
		return port.ErrRecordVersionConflict
	default:
		return err
	}
}
