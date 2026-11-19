package temporal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

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

	fromKeys := make([]string, len(in.OldNodeKeys))
	for i, k := range in.OldNodeKeys {
		fromKeys[i] = string(k)
	}
	direction := in.Direction
	if direction == "" {
		direction = domain.ForceRouteDirectionForward
	}

	ctx = withTenantGUC(ctx, tenantID)
	return retryableOrNotFound(d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		core := domain.CommonCore{WorkflowInstanceID: instanceID, SubjectID: inst.SubjectID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
		payload := domain.NewWorkflowInstanceForceRoutedPayload(core, adminUserID, fromKeys, in.TargetNodeID, direction)
		eventID := forceRouteEventID(instanceID, in)
		announced, err := d.eventRecorded(ctx, tenantID, eventID)
		if err != nil || announced {
			return err
		}
		if err := d.supersedeBypassedTasks(ctx, tenantID, instanceID, in.OldNodeKeys, adminUserID); err != nil {
			return err
		}
		return d.enqueueInstanceEventWithID(ctx, tenantID, instanceID, eventID, domain.EventWorkflowInstanceForceRouted, payload)
	}))
}

var forceRouteEventNamespace = uuid.MustParse("5a1f7d0e-3c2b-4e8a-9b6d-0f4c7a2e9d13")

// forceRouteEventID is the id of the force-routed event for one request. It is
// derived from the request, so an attempt that committed and its retry name the
// same event, while the same route made again at a later record version is a
// different event.
func forceRouteEventID(instanceID uuid.UUID, in port.RecordForceRouteInput) string {
	key := fmt.Sprintf("%s|%s|%s|%s|%d|%s", instanceID, in.Direction, in.TargetNodeID, in.AdminUserID, in.RecordVersion, joinNodeKeys(in.OldNodeKeys))
	return uuid.NewSHA1(forceRouteEventNamespace, []byte(key)).String()
}

func joinNodeKeys(keys []domain.NodeKey) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = string(k)
	}
	return strings.Join(parts, ",")
}

// eventRecorded reports whether the event with this id already exists, which
// means an earlier attempt of the same activity committed.
func (d *Deps) eventRecorded(ctx context.Context, tenantID uuid.UUID, eventID string) (bool, error) {
	id, err := uuid.Parse(eventID)
	if err != nil {
		return false, nonRetryable("ValidationError", fmt.Errorf("parse event id: %w", err))
	}
	recorded, err := d.Outbox.ExistsByID(ctx, tenantID, id)
	if err != nil {
		return false, fmt.Errorf("look up event %s: %w", id, err)
	}
	return recorded, nil
}

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
	assigneeUserIDs := []uuid.UUID{}
	for _, a := range active {
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
	payload := domain.NewWorkflowTaskSupersededPayload(core, taskCore, actorUserID)
	return d.enqueueInstanceEvent(ctx, tenantID, instanceID, domain.EventWorkflowTaskSuperseded, payload)
}

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
	recorded := false
	err = d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		task, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
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
		assigneeUserIDs := []uuid.UUID{}
		for _, a := range active {
			assigneeUserIDs = append(assigneeUserIDs, a.UserID)
		}
		core, err := d.instanceCore(ctx, tenantID, instanceID)
		if err != nil {
			return err
		}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
		if err := d.enqueueInstanceEvent(ctx, tenantID, instanceID, eventType, buildPayload(core, taskCore, task)); err != nil {
			return err
		}
		recorded = true
		return nil
	})
	if err == nil && recorded && eventType == domain.EventWorkflowTaskSLABreached {
		observability.IncCounter(observability.SLABreachesTotal)
	}
	return retryableOrNotFound(err)
}
