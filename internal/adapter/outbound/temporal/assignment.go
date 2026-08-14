package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// ClaimAssignment is ClaimAssignmentActivity (LLD §3.1): establishes the lead
// assignment on a multi-assignee (assignee_mode='all') task, then enqueues
// workflow.task.claimed. Only meaningful for assignee_mode='all' tasks;
// callers gate on that themselves (task-claim's own precondition).
//
// SetLead is now guarded by the task's own record_version (the LLD frames
// the task, not the assignment, as claim/complete's contested resource) and
// this activity no-ops — rather than reusing a stale version and retrying
// forever — when a prior attempt already made existing the lead.
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

// CompleteAssignment is CompleteAssignmentActivity (LLD §3.1): sets
// claimed_at/completed_at on the assignment, enqueues workflow.task.completed,
// and reports whether every assignment on the task has now completed.
//
// Complete is now guarded by the task's own record_version, same rationale
// as ClaimAssignment above. Idempotent under retry: no-ops (recomputing
// AllDone fresh, but skipping the event re-enqueue) when the assignment is
// already completed from a prior attempt, rather than reusing a stale
// version and retrying forever.
func (d *Deps) CompleteAssignment(ctx context.Context, in port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	assignmentID, err := uuid.Parse(in.AssignmentID)
	if err != nil {
		return port.CompleteAssignmentOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse assignment_id: %w", err))
	}

	var out port.CompleteAssignmentOutput
	ctx = withTenantGUC(ctx, tenantID)
	err = d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		existing, err := d.Assignments.GetByID(ctx, tenantID, assignmentID)
		if err != nil {
			return fmt.Errorf("get assignment: %w", err)
		}
		task, err := d.Tasks.GetByID(ctx, tenantID, existing.TaskID)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		if existing.CompletedAt != nil {
			active, err := d.Assignments.ListActiveByTask(ctx, tenantID, existing.TaskID)
			if err != nil {
				return fmt.Errorf("list active assignments: %w", err)
			}
			out.AllDone = len(active) == 0
			return nil
		}
		completed, err := d.Assignments.Complete(ctx, tenantID, assignmentID, []byte(in.ResultJSON), task.RecordVersion)
		if err != nil {
			return fmt.Errorf("complete assignment: %w", err)
		}
		active, err := d.Assignments.ListActiveByTask(ctx, tenantID, completed.TaskID)
		if err != nil {
			return fmt.Errorf("list active assignments: %w", err)
		}
		out.AllDone = len(active) == 0

		core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{completed.UserID}}
		payload := domain.NewWorkflowTaskCompletedPayload(core, taskCore, completed.UserID)
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskCompleted, payload)
	})
	if err != nil {
		return port.CompleteAssignmentOutput{}, err
	}
	return out, nil
}

// DeferTask is DeferTaskActivity (LLD §3.1): marks the task DEFERRED,
// completes the deferring assignment, creates a regression task (same
// department/node, fresh assignments from the same default-assignee set),
// and enqueues workflow.task.deferred.
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
	// The regression task's own assignee: no candidate other than the
	// deferrer is available on this input (DeferTaskInput carries no
	// DefaultAssignees) — signals.go's own handleStageDefer already flags
	// its Task/AssignmentID convention here as a T1.1 simplification
	// pending a real convention; this mirrors that same scope limit rather
	// than inventing a resolution DeferTaskInput doesn't support.
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
		// A retry after a lost ack: the prior attempt already deferred the
		// task and completed this assignment. createRegressionTask below is
		// itself idempotent (deterministic ID) and still needs to run to
		// resolve the (already-created) regression task for the output, but
		// the deferred-event enqueue is skipped since the first attempt's
		// event already went out.
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

// createRegressionTask inserts DeferTask's own replacement task (same
// department/node, one fresh assignment for assigneeID) and returns it.
//
// Idempotent under retry, same rationale as CreateTask: newTask.ID is
// derived deterministically from deferred.ID (not deferred.ID+NodeKey —
// deterministicTaskID would collide, since a regression task deliberately
// reuses its original's own NodeKey). A retry hits its own primary key,
// classified by mapErr as domain.ErrAlreadyExists; this fetches and returns
// the row a prior attempt already created instead of erroring.
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

// ReassignAssignment is ReassignAssignmentActivity (LLD §3.1): vacates the
// old assignment, inserts a new one for newUserID, and enqueues
// workflow.task.reassigned.
//
// The vacate loop is already idempotent under retry (a retried attempt's
// ListActiveByTask won't re-list an assignment the first attempt already
// vacated). The Create for newUserID was not: a retry after a lost ack
// would attempt a second insert, hit uq_workflow_task_assignment_active,
// and — same unclassified-error/unbounded-retry shape as CancelInstance
// above — retry forever. Checking active for an existing newUserID
// assignment first, and no-oping (including skipping the event re-enqueue,
// since the first attempt's event already went out) closes that.
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

// assignmentActiveFor reports whether userID already has an active
// assignment in active — ReassignAssignment's own idempotency check.
func assignmentActiveFor(active []*domain.TaskAssignment, userID uuid.UUID) bool {
	for _, a := range active {
		if a.UserID == userID {
			return true
		}
	}
	return false
}

// vacateAssignmentsFor vacates every active assignment belonging to userID —
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

// UpdateTaskStatus is UpdateTaskStatusActivity (chunk 8's stage-fail
// addition): a task-status-only transition, enqueuing workflow.task.failed
// when the new status is FAILED.
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
		task, err := d.Tasks.UpdateStatus(ctx, tenantID, taskID, in.Status, in.RecordVersion)
		if err != nil {
			return fmt.Errorf("update task status: %w", err)
		}
		if in.Status != domain.TaskStatusFailed {
			return nil
		}
		core := domain.CommonCore{WorkflowInstanceID: task.WorkflowInstanceID}
		taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID}
		payload := domain.NewWorkflowTaskFailedPayload(core, taskCore, "stage_fail")
		return d.enqueueInstanceEvent(ctx, tenantID, task.WorkflowInstanceID, domain.EventWorkflowTaskFailed, payload)
	})
}
