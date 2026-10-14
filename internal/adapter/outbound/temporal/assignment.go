package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func (d *Deps) ClaimAssignment(ctx context.Context, in port.ClaimAssignmentInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	assignmentID, err := uuid.Parse(in.AssignmentID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse assignment_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		existing, err := d.Assignments.GetByID(ctx, tenantID, assignmentID)
		if err != nil {
			return fmt.Errorf("get assignment: %w", err)
		}
		if existing.IsLead {
			return nil
		}
		task, err := d.Tasks.GetByID(ctx, tenantID, existing.TaskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		if _, err := d.Assignments.SetLead(ctx, tenantID, existing.TaskID, assignmentID, task.RecordVersion); err != nil {
			return fmt.Errorf("set lead: %w", err)
		}
		core, err := d.instanceCore(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			return err
		}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{existing.UserID}}
		payload := domain.NewWorkflowTaskClaimedPayload(core, taskCore, existing.UserID)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskClaimed, payload)
	})
}

func (d *Deps) CompleteAssignment(ctx context.Context, in port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	taskID, err := uuid.Parse(in.TaskID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse task_id: %w", err))
	}
	var userID uuid.UUID
	if in.UserID != "" {
		userID, err = uuid.Parse(in.UserID)
		if err != nil {
			return port.CompleteAssignmentOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse user_id: %w", err))
		}
	}

	var out port.CompleteAssignmentOutput
	ctx = withTenantGUC(ctx, tenantID)
	err = d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		task, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		if in.UserID == "" {
			return nil
		}
		out, err = d.completeUserAssignment(ctx, tenantID, task, userID, in.ResultJSON)
		return err
	})
	if err != nil {
		return port.CompleteAssignmentOutput{}, err
	}
	return out, nil
}

func (d *Deps) completeUserAssignment(ctx context.Context, tenantID uuid.UUID, task *domain.Task, userID uuid.UUID, resultJSON string) (port.CompleteAssignmentOutput, error) {
	active, err := d.Assignments.ListActiveByTask(ctx, tenantID, task.ID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, fmt.Errorf("list active assignments: %w", err)
	}
	target := assignmentFor(active, userID)
	if target == nil {
		return port.CompleteAssignmentOutput{AllDone: len(active) == 0}, nil
	}
	completed, err := d.Assignments.Complete(ctx, tenantID, target.ID, []byte(resultJSON), task.RecordVersion)
	if err != nil {
		return port.CompleteAssignmentOutput{}, fmt.Errorf("complete assignment: %w", err)
	}
	remaining, err := d.Assignments.ListActiveByTask(ctx, tenantID, completed.TaskID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, fmt.Errorf("list active assignments: %w", err)
	}
	out := port.CompleteAssignmentOutput{AllDone: len(remaining) == 0}

	core, err := d.instanceCore(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, err
	}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{completed.UserID}}
	payload := domain.NewWorkflowTaskCompletedPayload(core, taskCore, completed.UserID)
	if err := d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskCompleted, payload); err != nil {
		return port.CompleteAssignmentOutput{}, err
	}
	return out, nil
}

func (d *Deps) DeferTask(ctx context.Context, in port.DeferTaskInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	taskID, err := uuid.Parse(in.TaskID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse task_id: %w", err))
	}
	assignmentID, err := uuid.Parse(in.AssignmentID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse assignment_id: %w", err))
	}
	deferrerUserID, err := uuid.Parse(in.UserID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse user_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		task, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		existing, err := d.Assignments.GetByID(ctx, tenantID, assignmentID)
		if err != nil {
			return fmt.Errorf("get assignment: %w", err)
		}
		if existing.CompletedAt != nil {
			return nil
		}
		updatedTask, err := d.Tasks.UpdateStatus(ctx, tenantID, taskID, domain.TaskStatusDeferred, task.RecordVersion)
		if err != nil {
			return fmt.Errorf("mark task deferred: %w", err)
		}
		if _, err := d.Assignments.Complete(ctx, tenantID, assignmentID, nil, updatedTask.RecordVersion); err != nil {
			return fmt.Errorf("complete deferring assignment: %w", err)
		}

		core, err := d.instanceCore(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			return err
		}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{deferrerUserID}}
		reason := &in.Reason
		payload := domain.NewWorkflowTaskDeferredPayload(core, taskCore, in.DeferredToNodeKey, reason, nil)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskDeferred, payload)
	})
}

func (d *Deps) ReassignAssignment(ctx context.Context, in port.ReassignAssignmentInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	taskID, err := uuid.Parse(in.TaskID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse task_id: %w", err))
	}
	oldUserID, err := uuid.Parse(in.OldUserID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse old_user_id: %w", err))
	}
	newUserID, err := uuid.Parse(in.NewUserID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse new_user_id: %w", err))
	}
	adminUserID, err := uuid.Parse(in.AdminUserID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse admin_user_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		task, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		active, err := d.Assignments.ListActiveByTask(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("list active assignments: %w", err)
		}
		if assignmentActiveFor(active, newUserID) {
			return nil
		}
		if _, err := d.Tasks.BumpRecordVersion(ctx, tenantID, taskID, in.RecordVersion); err != nil {
			if errors.Is(err, domain.ErrRecordVersionConflict) {
				return nonRetryable("VersionConflict", fmt.Errorf("reassign: lost the record_version race: %w", err))
			}
			return fmt.Errorf("bump task record version: %w", err)
		}
		if err := vacateAssignmentsFor(ctx, d.Assignments, tenantID, active, oldUserID); err != nil {
			return err
		}
		assignment := &domain.TaskAssignment{
			ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: newUserID,
			AssignedBy: &adminUserID, IsLead: leadHeldBy(active, oldUserID),
		}
		if err := d.Assignments.Create(ctx, assignment); err != nil {
			return fmt.Errorf("create new assignment: %w", err)
		}

		core, err := d.instanceCore(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			return err
		}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{newUserID}}
		payload := domain.NewWorkflowTaskReassignedPayload(core, taskCore, oldUserID, newUserID, domain.ReassignInitiatorAdmin, nil)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskReassigned, payload)
	})
}

func leadHeldBy(active []*domain.TaskAssignment, userID uuid.UUID) bool {
	a := assignmentFor(active, userID)
	return a != nil && a.IsLead
}

func assignmentActiveFor(active []*domain.TaskAssignment, userID uuid.UUID) bool {
	return assignmentFor(active, userID) != nil
}

func assignmentFor(active []*domain.TaskAssignment, userID uuid.UUID) *domain.TaskAssignment {
	for _, a := range active {
		if a.UserID == userID {
			return a
		}
	}
	return nil
}

func vacateAssignmentsFor(ctx context.Context, repo port.TaskAssignmentRepository, tenantID uuid.UUID, active []*domain.TaskAssignment, userID uuid.UUID) error {
	for _, a := range active {
		if a.UserID != userID {
			continue
		}
		if _, err := repo.Vacate(ctx, tenantID, a.ID); err != nil {
			return fmt.Errorf("vacate old assignment: %w", err)
		}
	}
	return nil
}

func (d *Deps) supersedeAbandoned(ctx context.Context, tenantID uuid.UUID, task *domain.Task) error {
	if task.Status != domain.TaskStatusReady && task.Status != domain.TaskStatusInProgress {
		return nil
	}
	return d.supersedeTask(ctx, tenantID, task.WorkflowInstanceID, task, uuid.Nil)
}

func (d *Deps) UpdateTaskStatus(ctx context.Context, in port.UpdateTaskStatusInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	taskID, err := uuid.Parse(in.TaskID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse task_id: %w", err))
	}

	ctx = withTenantGUC(ctx, tenantID)
	return d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		existing, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		if existing.Status == in.Status {
			return nil
		}
		if in.Status == domain.TaskStatusSuperseded {
			return d.supersedeAbandoned(ctx, tenantID, existing)
		}
		task, err := d.Tasks.UpdateStatus(ctx, tenantID, taskID, in.Status, existing.RecordVersion)
		if err != nil {
			return fmt.Errorf("update task status: %w", err)
		}
		if in.Status != domain.TaskStatusFailed {
			return nil
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
		core, err := d.instanceCore(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			return err
		}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
		payload := domain.NewWorkflowTaskFailedPayload(core, taskCore, "stage_fail", in.ErrorClass)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskFailed, payload)
	})
}
