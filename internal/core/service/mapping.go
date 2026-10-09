package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func toPortInstance(inst *domain.Instance) *port.Instance {
	return &port.Instance{
		ID:                 inst.ID,
		TenantID:           inst.TenantID,
		WorkflowID:         inst.WorkflowID,
		WorkflowVersionID:  inst.WorkflowVersionID,
		SubjectID:          inst.SubjectID,
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

type outboxEnvelope struct {
	TenantID string          `json:"tenant_id"`
	Actor    string          `json:"actor"`
	Data     json.RawMessage `json:"data"`
}

type taskScopedFields struct {
	TaskID  uuid.UUID `json:"task_id"`
	NodeKey string    `json:"node_key"`
}

var errUnknownEventType = errors.New("unrecognized outbox event type")

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
	_ = json.Unmarshal(envelope.Data, &scoped)

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

func wrapInstanceErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return port.ErrInstanceNotFound
	case errors.Is(err, domain.ErrRecordVersionConflict):
		observability.IncCounterVec(observability.RecordVersionConflictTotal, "workflow_instance")
		return port.ErrRecordVersionConflict
	case errors.Is(err, domain.ErrDuplicateBusinessKey):
		return port.ErrDuplicateBusinessKey
	default:
		return err
	}
}

func wrapTaskErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return port.ErrTaskNotFound
	case errors.Is(err, domain.ErrRecordVersionConflict):
		observability.IncCounterVec(observability.RecordVersionConflictTotal, "workflow_task")
		return port.ErrRecordVersionConflict
	default:
		return err
	}
}
