package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// RecordForceRoute is RecordForceRouteActivity (LLD §3.1): marks the task(s)
// bypassed by an instance-force-forward SUPERSEDED, vacates their
// assignments, and writes one workflow.task.superseded per bypassed task
// plus one workflow.instance.force-routed for the instance.
func (d *Deps) RecordForceRoute(ctx context.Context, in port.RecordForceRouteInput) error {
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
		if err := d.supersedeBypassedTasks(ctx, tenantID, instanceID, in.OldNodeKeys, adminUserID); err != nil {
			return err
		}
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
		fromKeys := make([]string, len(in.OldNodeKeys))
		for i, k := range in.OldNodeKeys {
			fromKeys[i] = string(k)
		}
		payload := domain.NewWorkflowInstanceForceRoutedPayload(core, adminUserID, fromKeys, in.TargetNodeID, domain.ForceRouteDirectionForward)
		return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowInstanceForceRouted, payload)
	})
}

// supersedeBypassedTasks marks every still-open task at one of oldNodeKeys
// SUPERSEDED, vacating its active assignments, and enqueues one
// workflow.task.superseded per task.
func (d *Deps) supersedeBypassedTasks(ctx context.Context, tenantID, instanceID uuid.UUID, oldNodeKeys []domain.NodeKey, actorUserID uuid.UUID) error {
	bypassed := make(map[string]bool, len(oldNodeKeys))
	for _, k := range oldNodeKeys {
		bypassed[string(k)] = true
	}
	var cursor *port.Cursor
	for {
		tasks, next, err := d.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: cursor, Limit: 50})
		if err != nil {
			return fmt.Errorf("list tasks by instance: %w", err)
		}
		for _, task := range tasks {
			if !bypassed[task.NodeKey] || (task.Status != domain.TaskStatusReady && task.Status != domain.TaskStatusInProgress) {
				continue
			}
			if err := d.supersedeTask(ctx, tenantID, instanceID, task, actorUserID); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		cursor = next
	}
}

func (d *Deps) supersedeTask(ctx context.Context, tenantID, instanceID uuid.UUID, task *domain.Task, actorUserID uuid.UUID) error {
	if _, err := d.Tasks.UpdateStatus(ctx, tenantID, task.ID, domain.TaskStatusSuperseded, task.RecordVersion); err != nil {
		return fmt.Errorf("supersede task %s: %w", task.ID, err)
	}
	active, err := d.Assignments.ListActiveByTask(ctx, tenantID, task.ID)
	if err != nil {
		return fmt.Errorf("list active assignments for task %s: %w", task.ID, err)
	}
	var assigneeUserIDs []uuid.UUID
	for _, a := range active {
		if _, err := d.Assignments.Vacate(ctx, tenantID, a.ID); err != nil {
			return fmt.Errorf("vacate assignment %s: %w", a.ID, err)
		}
		assigneeUserIDs = append(assigneeUserIDs, a.UserID)
	}
	core := domain.CommonCore{WorkflowInstanceID: instanceID}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
	payload := domain.NewWorkflowTaskSupersededPayload(core, taskCore, actorUserID)
	return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowTaskSuperseded, payload)
}

// RecordSLAWarning is RecordSLAWarningActivity (LLD §3.1): audit-only, no
// status change — enqueues workflow.task.sla-warning.
func (d *Deps) RecordSLAWarning(ctx context.Context, in port.RecordSLAWarningInput) error {
	return d.recordSLAEvent(ctx, in.TenantID, in.InstanceID, in.TaskID, domain.EventWorkflowTaskSLAWarning,
		func(core domain.CommonCore, taskCore domain.TaskScopedCore, task *domain.Task) any {
			followUpAt := time.Time{}
			if task.FollowUpAt != nil {
				followUpAt = *task.FollowUpAt
			}
			return domain.NewWorkflowTaskSLAWarningPayload(core, taskCore, followUpAt)
		})
}

// RecordSLABreach is RecordSLABreachActivity (LLD §3.1): audit-only, no
// status change — enqueues workflow.task.sla-breached.
func (d *Deps) RecordSLABreach(ctx context.Context, in port.RecordSLABreachInput) error {
	return d.recordSLAEvent(ctx, in.TenantID, in.InstanceID, in.TaskID, domain.EventWorkflowTaskSLABreached,
		func(core domain.CommonCore, taskCore domain.TaskScopedCore, task *domain.Task) any {
			dueAt := time.Time{}
			if task.DueAt != nil {
				dueAt = *task.DueAt
			}
			return domain.NewWorkflowTaskSLABreachedPayload(core, taskCore, dueAt)
		})
}

func (d *Deps) recordSLAEvent(
	ctx context.Context, tenantIDStr, instanceIDStr, taskIDStr, eventType string,
	buildPayload func(core domain.CommonCore, taskCore domain.TaskScopedCore, task *domain.Task) any,
) error {
	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	instanceID, err := uuid.Parse(instanceIDStr)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse instance_id: %w", err))
	}
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse task_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		task, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		// Audit-only: no status mutation this retry could gate on, so a
		// retried attempt checks the outbox itself for whether it already
		// recorded this exact event for this task, rather than
		// double-emitting workflow.task.sla-warning/sla-breached.
		exists, err := d.Outbox.ExistsForTask(ctx, eventType, taskID)
		if err != nil {
			return fmt.Errorf("check existing sla event: %w", err)
		}
		if exists {
			return nil
		}
		active, err := d.Assignments.ListActiveByTask(ctx, tenantID, task.ID)
		if err != nil {
			return fmt.Errorf("list active assignments for task %s: %w", task.ID, err)
		}
		var assigneeUserIDs []uuid.UUID
		for _, a := range active {
			assigneeUserIDs = append(assigneeUserIDs, a.UserID)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
		return d.enqueueInstanceEvent(ctx, tenantID, instanceID, eventType, buildPayload(core, taskCore, task))
	})
}
