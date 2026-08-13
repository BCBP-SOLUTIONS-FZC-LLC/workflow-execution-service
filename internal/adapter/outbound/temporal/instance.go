package temporal

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// UpdateInstanceStatus is UpdateInstanceStatusActivity (LLD §3.1). Completed
// enqueues workflow.instance.finished; Failed also cascades every still-open
// task to FAILED (vacating their assignments) before enqueuing
// workflow.instance.failed. Degraded and a Degraded->Running recovery update
// the status column only, with no event: UpdateInstanceStatusInput carries
// neither FailedBranches (workflow.instance.degraded's own payload) nor an
// error class, and inventing either here would fabricate data the
// interpreter never actually captured — a real gap in that input's shape,
// not something to paper over unilaterally in this activity.
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
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		updated, err := d.Instances.UpdateStatus(ctx, tenantID, instanceID, in.Status, inst.RecordVersion)
		if err != nil {
			return fmt.Errorf("update instance status: %w", err)
		}

		core := domain.CommonCore{WorkflowInstanceID: instanceID, BusinessKey: updated.BusinessKey, WorkflowVersionID: updated.WorkflowVersionID}
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

		case domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusTerminated, domain.InstanceStatusDegraded:
			// No event: Degraded/Running-from-Degraded have no input data to
			// build a real payload from (see this method's own doc comment);
			// Paused/Terminated/Running are never reached through this
			// activity (they have their own dedicated activities below).
			return nil
		default:
			return nil
		}
	})
}

// PauseInstance is PauseInstanceActivity (LLD §3.1).
func (d *Deps) PauseInstance(ctx context.Context, in port.PauseInstanceInput) error {
	return d.instanceLifecycleEvent(ctx, in.InstanceID, in.TenantID, in.RecordVersion, domain.InstanceStatusPaused,
		func(core domain.CommonCore, startedByUserID uuid.UUID) any {
			return domain.NewWorkflowInstancePausedPayload(core, startedByUserID, domain.InitiatorAdmin, adminUserIDPtr(in.AdminUserID))
		}, domain.EventWorkflowInstancePaused)
}

// ResumeInstance is ResumeInstanceActivity (LLD §3.1).
func (d *Deps) ResumeInstance(ctx context.Context, in port.ResumeInstanceInput) error {
	return d.instanceLifecycleEvent(ctx, in.InstanceID, in.TenantID, in.RecordVersion, domain.InstanceStatusRunning,
		func(core domain.CommonCore, startedByUserID uuid.UUID) any {
			return domain.NewWorkflowInstanceResumedPayload(core, startedByUserID, domain.InitiatorAdmin, adminUserIDPtr(in.AdminUserID))
		}, domain.EventWorkflowInstanceResumed)
}

// CancelInstance is CancelInstanceActivity (LLD §3.1): marks every still-open
// task FAILED (vacating their assignments), updates status to TERMINATED,
// and writes both event classes — the per-task workflow.task.failed cascade,
// then workflow.instance.terminated.
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
		if err := d.failActiveTasks(ctx, tenantID, instanceID, "instance_cancel"); err != nil {
			return err
		}
		updated, err := d.Instances.UpdateStatus(ctx, tenantID, instanceID, domain.InstanceStatusTerminated, in.RecordVersion)
		if err != nil {
			return fmt.Errorf("update instance status: %w", err)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID, BusinessKey: updated.BusinessKey, WorkflowVersionID: updated.WorkflowVersionID}
		payload := domain.NewWorkflowInstanceTerminatedPayload(core, updated.StartedByUserID, domain.TerminatedInitiatorAdmin, &adminUserID)
		return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceTerminated, payload)
	})
}

// instanceLifecycleEvent is Pause/ResumeInstanceActivity's shared shape: a
// version-checked status update followed by one instance-lifecycle event.
func (d *Deps) instanceLifecycleEvent(
	ctx context.Context, instanceIDStr, tenantIDStr string, recordVersion int64, status domain.InstanceStatus,
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
		updated, err := d.Instances.UpdateStatus(ctx, tenantID, instanceID, status, recordVersion)
		if err != nil {
			return fmt.Errorf("update instance status: %w", err)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID, BusinessKey: updated.BusinessKey, WorkflowVersionID: updated.WorkflowVersionID}
		return d.enqueueInstanceEvent(ctx, tenantID, instanceID, eventType, buildPayload(core, updated.StartedByUserID))
	})
}

// failActiveTasks cascades every still-open (READY/IN_PROGRESS) task under
// instanceID to FAILED, vacating its active assignments, and enqueues one
// workflow.task.failed per task — shared by UpdateInstanceStatus(FAILED) and
// CancelInstance. Must be called from inside the caller's own RunInTx.
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
	var assigneeUserIDs []uuid.UUID
	for _, a := range assignments {
		if _, err := d.Assignments.Vacate(ctx, tenantID, a.ID); err != nil {
			return fmt.Errorf("vacate assignment %s: %w", a.ID, err)
		}
		assigneeUserIDs = append(assigneeUserIDs, a.UserID)
	}

	core := domain.CommonCore{WorkflowInstanceID: instanceID}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
	payload := domain.NewWorkflowTaskFailedPayload(core, taskCore, cascadeSource)
	return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowTaskFailed, payload)
}

// enqueueInstanceEvent is BuildEnvelope+Outbox.Enqueue's shared shape for
// every instance- or task-scoped event this file writes.
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
