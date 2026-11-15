package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func activeAssignmentByID(active []*domain.TaskAssignment, id uuid.UUID) bool {
	for _, a := range active {
		if a.ID == id {
			return true
		}
	}
	return false
}

func activeAssignmentOfUser(active []*domain.TaskAssignment, userID uuid.UUID) *domain.TaskAssignment {
	for _, a := range active {
		if a.UserID == userID {
			return a
		}
	}
	return nil
}

// vacateIfActive vacates the assignment inside the caller's transaction, after
// re-reading the task's active assignments there. The re-read is not locked, so
// the vacate itself only matches a row that is still active: an assignee who
// completes the task in between ends the row first. It returns the task's
// active assignments and false, with nothing written, when the assignment has
// already ended.
func vacateIfActive(ctx context.Context, repo port.TaskAssignmentRepository, tenantID uuid.UUID, a *domain.TaskAssignment) ([]*domain.TaskAssignment, bool, error) {
	active, err := repo.ListActiveByTask(ctx, tenantID, a.TaskID)
	if err != nil {
		return nil, false, err
	}
	if !activeAssignmentByID(active, a.ID) {
		return nil, false, nil
	}
	if _, err := repo.VacateActive(ctx, tenantID, a.ID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return active, true, nil
}

// handOver gives the vacated assignment's task to newUserID inside the caller's
// transaction. A user who already holds the task keeps that one row, and
// inherits the lead flag; inserting a second row would be a unique violation,
// which aborts the whole transaction.
func handOver(ctx context.Context, repo port.TaskAssignmentRepository, tenantID uuid.UUID, vacated *domain.TaskAssignment, active []*domain.TaskAssignment, task *domain.Task, newUserID uuid.UUID, reason string) error {
	if existing := activeAssignmentOfUser(active, newUserID); existing != nil {
		if !vacated.IsLead {
			return nil
		}
		_, err := repo.SetLead(ctx, tenantID, task.ID, existing.ID, task.RecordVersion)
		return err
	}
	return repo.Create(ctx, &domain.TaskAssignment{
		ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: newUserID,
		Reason: reason, IsLead: vacated.IsLead,
	})
}

// enqueueTaskReassigned records the WorkflowTaskReassigned event of a delegation
// move in the caller's transaction.
func enqueueTaskReassigned(ctx context.Context, outbox port.OutboxRepository, validator port.EventValidator, inst *domain.Instance, task *domain.Task, oldUserID, newUserID uuid.UUID, delegationID *uuid.UUID) error {
	core := domain.CommonCore{WorkflowInstanceID: inst.ID, SubjectID: inst.SubjectID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{newUserID}}
	payload := domain.NewWorkflowTaskReassignedPayload(core, taskCore, oldUserID, newUserID, domain.ReassignInitiatorDelegation, delegationID)
	subject := "instances/" + inst.ID.String() + "/tasks/" + task.ID.String()
	envelope, err := BuildEnvelope(ctx, validator, domain.EventWorkflowTaskReassigned, inst.TenantID.String(), subject, "", payload)
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, envelope)
}

// recordWorkflowSignal records a signal for the instance in the caller's
// transaction; the outbox relay delivers it to Temporal after the commit.
func recordWorkflowSignal(ctx context.Context, outbox port.WorkflowSignalOutbox, inst *domain.Instance, name string, wire any) error {
	payload, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshal %s signal: %w", name, err)
	}
	return outbox.EnqueueSignal(ctx, port.WorkflowSignal{
		TenantID: inst.TenantID, InstanceID: inst.ID, TemporalWorkflowID: inst.TemporalWorkflowID,
		Name: name, Payload: payload,
	})
}
