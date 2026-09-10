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
		core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
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
			out.AllDone = true
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

	core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{completed.UserID}}
	payload := domain.NewWorkflowTaskCompletedPayload(core, taskCore, completed.UserID)
	if err := d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskCompleted, payload); err != nil {
		return port.CompleteAssignmentOutput{}, err
	}
	return out, nil
}

func (d *Deps) DeferTask(ctx context.Context, in port.DeferTaskInput) (port.DeferTaskOutput, error) {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return port.DeferTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	taskID, err := uuid.Parse(in.TaskID)
	if err != nil {
		return port.DeferTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse task_id: %w", err))
	}
	assignmentID, err := uuid.Parse(in.AssignmentID)
	if err != nil {
		return port.DeferTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse assignment_id: %w", err))
	}
	deferrerUserID, err := uuid.Parse(in.UserID)
	if err != nil {
		return port.DeferTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse user_id: %w", err))
	}

	var newTask domain.Task
	ctx = withTenantGUC(ctx, tenantID)
	err = d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		task, err := d.Tasks.GetByID(ctx, tenantID, taskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		existing, err := d.Assignments.GetByID(ctx, tenantID, assignmentID)
		if err != nil {
			return fmt.Errorf("get assignment: %w", err)
		}
		// A retry after a lost ack skips re-deferring and re-completing, but
		// still resolves the regression task below for the output.
		alreadyDeferred := existing.CompletedAt != nil
		if !alreadyDeferred {
			updatedTask, err := d.Tasks.UpdateStatus(ctx, tenantID, taskID, domain.TaskStatusDeferred, task.RecordVersion)
			if err != nil {
				return fmt.Errorf("mark task deferred: %w", err)
			}
			if _, err := d.Assignments.Complete(ctx, tenantID, assignmentID, nil, updatedTask.RecordVersion); err != nil {
				return fmt.Errorf("complete deferring assignment: %w", err)
			}
		}
		created, err := d.createRegressionTask(ctx, tenantID, task, deferrerUserID)
		if err != nil {
			return err
		}
		newTask = *created
		if alreadyDeferred {
			return nil
		}

		core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{deferrerUserID}}
		reason := &in.Reason
		payload := domain.NewWorkflowTaskDeferredPayload(core, taskCore, newTask.NodeKey, reason, nil)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskDeferred, payload)
	})
	if err != nil {
		return port.DeferTaskOutput{}, err
	}
	return port.DeferTaskOutput{NewTaskID: newTask.ID.String()}, nil
}

func (d *Deps) createRegressionTask(ctx context.Context, tenantID uuid.UUID, deferred *domain.Task, assigneeID uuid.UUID) (*domain.Task, error) {
	newTaskID := deterministicRegressionTaskID(deferred.ID)
	newTask := &domain.Task{
		ID:                 newTaskID,
		TenantID:           tenantID,
		WorkflowInstanceID: deferred.WorkflowInstanceID,
		NodeKey:            deferred.NodeKey,
		DepartmentID:       deferred.DepartmentID,
		Status:             domain.TaskStatusReady,
		AssigneeMode:       deferred.AssigneeMode,
		DeferredFromTaskID: &deferred.ID,
		// Continues the same in-flight runTaskStage visit the deferred task
		// belonged to (the interpreter never re-registers on a defer — see
		// signals.go's handleStageDefer) — the eventual completion signal's
		// VisitCount must match the still-pending registration's, or it
		// resolves nothing and the workflow hangs.
		VisitCount: deferred.VisitCount,
	}
	if err := d.Tasks.Create(ctx, newTask); err != nil {
		if errors.Is(err, domain.ErrAlreadyExists) {
			existing, getErr := d.Tasks.GetByID(ctx, tenantID, newTaskID)
			if getErr != nil {
				return nil, fmt.Errorf("get existing regression task: %w", getErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("create regression task: %w", err)
	}
	assignment := &domain.TaskAssignment{ID: deterministicAssignmentID(newTask.ID, assigneeID), TenantID: tenantID, TaskID: newTask.ID, UserID: assigneeID}
	if err := d.Assignments.Create(ctx, assignment); err != nil {
		if errors.Is(err, domain.ErrAlreadyExists) {
			return newTask, nil
		}
		return nil, fmt.Errorf("create regression assignment: %w", err)
	}
	return newTask, nil
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
			// Already reassigned by a prior attempt whose ack was lost —
			// idempotent no-op, including skipping the event re-enqueue.
			return nil
		}
		// Two concurrent reassign requests can both pass their own version
		// check upstream before either commits; this bump lets only the
		// first through — the second gets a permanent, non-retryable
		// rejection rather than corrupting the assignment set.
		if _, err := d.Tasks.BumpRecordVersion(ctx, tenantID, taskID, in.RecordVersion); err != nil {
			if errors.Is(err, domain.ErrRecordVersionConflict) {
				return nonRetryable("VersionConflict", fmt.Errorf("reassign: lost the record_version race: %w", err))
			}
			return fmt.Errorf("bump task record version: %w", err)
		}
		if err := vacateAssignmentsFor(ctx, d.Assignments, tenantID, active, oldUserID); err != nil {
			return err
		}
		assignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: newUserID, AssignedBy: &adminUserID}
		if err := d.Assignments.Create(ctx, assignment); err != nil {
			return fmt.Errorf("create new assignment: %w", err)
		}

		core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{newUserID}}
		payload := domain.NewWorkflowTaskReassignedPayload(core, taskCore, oldUserID, newUserID, domain.ReassignInitiatorAdmin, nil)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskReassigned, payload)
	})
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

// naturally idempotent under retry, since a retried attempt's active list
// won't include an assignment a prior attempt already vacated.
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
			assigneeUserIDs = append(assigneeUserIDs, a.UserID)
		}
		core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
		payload := domain.NewWorkflowTaskFailedPayload(core, taskCore, "stage_fail")
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskFailed, payload)
	})
}
