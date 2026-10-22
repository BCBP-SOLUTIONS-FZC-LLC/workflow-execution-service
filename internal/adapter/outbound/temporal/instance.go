package temporal

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func (d *Deps) UpdateInstanceStatus(ctx context.Context, in port.UpdateInstanceStatusInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	instanceID, err := uuid.Parse(in.InstanceID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse instance_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	enteredDegraded := false
	err = d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		if (in.Status == domain.InstanceStatusDegraded || in.Status == domain.InstanceStatusRunning) && inst.Status == in.Status {
			return nil
		}
		updated, err := d.Instances.UpdateStatus(ctx, tenantID, instanceID, in.Status, inst.RecordVersion)
		if err != nil {
			return fmt.Errorf("update instance status: %w", err)
		}

		core := domain.CommonCore{WorkflowInstanceID: instanceID, SubjectID: updated.SubjectID, BusinessKey: updated.BusinessKey, WorkflowVersionID: updated.WorkflowVersionID}
		switch in.Status {
		case domain.InstanceStatusCompleted:
			completedAt := updated.UpdatedAt
			if in.CompletedAt != nil {
				completedAt = *in.CompletedAt
			}
			payload := domain.NewWorkflowInstanceFinishedPayload(core, updated.StartedByUserID, completedAt)
			return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceFinished, payload)

		case domain.InstanceStatusFailed:
			if err := d.failActiveTasks(ctx, tenantID, instanceID, "instance_failed"); err != nil {
				return err
			}
			payload := domain.NewWorkflowInstanceFailedPayload(core, "workflow_error")
			return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceFailed, payload)

		case domain.InstanceStatusDegraded:
			payload := domain.NewWorkflowInstanceDegradedPayload(core, in.FailedBranches)
			if err := d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceDegraded, payload); err != nil {
				return err
			}
			enteredDegraded = true
			return nil

		case domain.InstanceStatusRunning:
			payload := domain.NewWorkflowInstanceResumedPayload(core, updated.StartedByUserID, domain.InitiatorDegradedRecovery, nil)
			return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceResumed, payload)

		case domain.InstanceStatusPaused, domain.InstanceStatusTerminated:
			return nil
		default:
			return nil
		}
	})
	if err == nil && enteredDegraded {
		observability.IncCounter(observability.InstanceDegradedTotal)
	}
	return err
}

func (d *Deps) PauseInstance(ctx context.Context, in port.PauseInstanceInput) error {
	return d.instanceLifecycleEvent(ctx, in.InstanceID, in.TenantID, in.RecordVersion, domain.InstanceStatusPaused,
		func(core domain.CommonCore, startedByUserID uuid.UUID) any {
			return domain.NewWorkflowInstancePausedPayload(core, startedByUserID, in.Initiator, adminUserIDPtr(in.AdminUserID))
		}, domain.EventWorkflowInstancePaused)
}

func (d *Deps) ResumeInstance(ctx context.Context, in port.ResumeInstanceInput) error {
	return d.instanceLifecycleEvent(ctx, in.InstanceID, in.TenantID, in.RecordVersion, domain.InstanceStatusRunning,
		func(core domain.CommonCore, startedByUserID uuid.UUID) any {
			return domain.NewWorkflowInstanceResumedPayload(core, startedByUserID, in.Initiator, adminUserIDPtr(in.AdminUserID))
		}, domain.EventWorkflowInstanceResumed)
}

func (d *Deps) CancelInstance(ctx context.Context, in port.CancelInstanceInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	instanceID, err := uuid.Parse(in.InstanceID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse instance_id: %w", err))
	}
	adminUserID, err := uuid.Parse(in.AdminUserID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse admin_user_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		if inst.Status == domain.InstanceStatusTerminated {
			return nil
		}
		if err := d.failActiveTasks(ctx, tenantID, instanceID, "instance_cancel"); err != nil {
			return err
		}
		updated, err := d.Instances.UpdateStatus(ctx, tenantID, instanceID, domain.InstanceStatusTerminated, inst.RecordVersion)
		if err != nil {
			return fmt.Errorf("update instance status: %w", err)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID, SubjectID: updated.SubjectID, BusinessKey: updated.BusinessKey, WorkflowVersionID: updated.WorkflowVersionID}
		payload := domain.NewWorkflowInstanceCancelledPayload(core, updated.StartedByUserID, adminUserID, in.Reason)
		return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceCancelled, payload)
	})
}

func (d *Deps) instanceLifecycleEvent(
	ctx context.Context, instanceIDStr, tenantIDStr string, _ int64, status domain.InstanceStatus,
	buildPayload func(core domain.CommonCore, startedByUserID uuid.UUID) any, eventType string,
) error {
	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	instanceID, err := uuid.Parse(instanceIDStr)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse instance_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		if inst.Status == status {
			return nil
		}
		updated, err := d.Instances.UpdateStatus(ctx, tenantID, instanceID, status, inst.RecordVersion)
		if err != nil {
			return fmt.Errorf("update instance status: %w", err)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID, SubjectID: updated.SubjectID, BusinessKey: updated.BusinessKey, WorkflowVersionID: updated.WorkflowVersionID}
		return d.enqueueInstanceEvent(ctx, tenantID, instanceID, eventType, buildPayload(core, updated.StartedByUserID))
	})
}

func (d *Deps) failActiveTasks(ctx context.Context, tenantID, instanceID uuid.UUID, cascadeSource string) error {
	var cursor *port.Cursor
	for {
		tasks, next, err := d.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: cursor, Limit: 50})
		if err != nil {
			return fmt.Errorf("list tasks by instance: %w", err)
		}
		for _, task := range tasks {
			if task.Status != domain.TaskStatusReady && task.Status != domain.TaskStatusInProgress {
				continue
			}
			if err := d.failTask(ctx, tenantID, instanceID, task, cascadeSource); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		cursor = next
	}
}

func (d *Deps) failTask(ctx context.Context, tenantID, instanceID uuid.UUID, task *domain.Task, cascadeSource string) error {
	if _, err := d.Tasks.UpdateStatus(ctx, tenantID, task.ID, domain.TaskStatusFailed, task.RecordVersion); err != nil {
		return fmt.Errorf("fail task %s: %w", task.ID, err)
	}
	assignments, err := d.Assignments.ListActiveByTask(ctx, tenantID, task.ID)
	if err != nil {
		return fmt.Errorf("list active assignments for task %s: %w", task.ID, err)
	}
	assigneeUserIDs := []uuid.UUID{}
	for _, a := range assignments {
		if _, err := d.Assignments.Vacate(ctx, tenantID, a.ID); err != nil {
			return fmt.Errorf("vacate assignment %s: %w", a.ID, err)
		}
		assigneeUserIDs = append(assigneeUserIDs, a.UserID)
	}

	core, err := d.instanceCore(ctx, tenantID, instanceID)
	if err != nil {
		return err
	}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
	payload := domain.NewWorkflowTaskFailedPayload(core, taskCore, cascadeSource, "")
	return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowTaskFailed, payload)
}

func (d *Deps) enqueueInstanceEvent(ctx context.Context, tenantID, instanceID uuid.UUID, eventType string, payload any) error {
	env, err := service.BuildEnvelope(ctx, d.Validator, eventType, tenantID.String(), "instances/"+instanceID.String(), "", payload)
	if err != nil {
		return fmt.Errorf("build %s envelope: %w", eventType, err)
	}
	if err := d.Outbox.Enqueue(ctx, env); err != nil {
		return fmt.Errorf("enqueue %s: %w", eventType, err)
	}
	return nil
}

func adminUserIDPtr(s string) *uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		return nil
	}
	return &id
}
