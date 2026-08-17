package service

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.WorkflowClient = (*WorkflowClient)(nil)

// WorkflowClient implements port.WorkflowClient (LLD §5.8): the
// scope-aware filter algorithm every method shares — establish a delegate's
// active assignments, optionally narrowed to one delegation's own
// reason="delegation:<id>" tag — then act. ReassignDelegate/CancelByDelegate
// mutate; DelegateImpact only previews.
type WorkflowClient struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Temporal    port.TemporalClient
	Log         port.Logger
}

func (s *WorkflowClient) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func delegationReasonTag(delegationID uuid.UUID) string {
	return "delegation:" + delegationID.String()
}

// matchingAssignments is the shared scope-aware filter: every active
// assignment for the delegate, narrowed to one delegation's own reason tag
// when delegationID is supplied — omitted, every active assignment for that
// delegate counts (LLD §5.8's own documented fallback).
func matchingAssignments(assignments []*domain.TaskAssignment, delegationID *uuid.UUID) []*domain.TaskAssignment {
	if delegationID == nil {
		return assignments
	}
	tag := delegationReasonTag(*delegationID)
	var out []*domain.TaskAssignment
	for _, a := range assignments {
		if strings.HasPrefix(a.Reason, tag) {
			out = append(out, a)
		}
	}
	return out
}

func (s *WorkflowClient) ReassignDelegate(ctx context.Context, in port.ReassignDelegateInput) (int, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.OldDelegateID)
	if err != nil {
		return 0, err
	}
	matched := matchingAssignments(active, in.DelegationID)

	count := 0
	for _, a := range matched {
		task, err := s.Tasks.GetByID(ctx, in.TenantID, a.TaskID)
		if err != nil {
			s.logger().Warn("skipping assignment with unreadable task", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		if _, err := s.Assignments.Vacate(ctx, in.TenantID, a.ID); err != nil {
			s.logger().Warn("failed to vacate assignment during delegate reassignment", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		newAssignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: in.TenantID, TaskID: a.TaskID, UserID: in.NewDelegateID, Reason: a.Reason}
		if err := s.Assignments.Create(ctx, newAssignment); err != nil {
			s.logger().Warn("failed to create replacement assignment during delegate reassignment", map[string]any{"task_id": a.TaskID, "error": err.Error()})
			continue
		}
		if err := s.signalReassignForTask(ctx, task, a.UserID, in.NewDelegateID); err != nil {
			s.logger().Warn("failed to signal instance-reassign, DB state already updated", map[string]any{"task_id": a.TaskID, "error": err.Error()})
		}
		count++
	}
	return count, nil
}

func (s *WorkflowClient) CancelByDelegate(ctx context.Context, in port.CancelByDelegateInput) (int, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateUserID)
	if err != nil {
		return 0, err
	}
	matched := matchingAssignments(active, in.DelegationID)

	count := 0
	for _, a := range matched {
		if _, err := s.Assignments.Vacate(ctx, in.TenantID, a.ID); err != nil {
			s.logger().Warn("failed to vacate assignment during delegate cancellation", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		count++
	}
	return count, nil
}

func (s *WorkflowClient) DelegateImpact(ctx context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateUserID)
	if err != nil {
		return port.DelegateImpactResult{}, err
	}
	matched := matchingAssignments(active, in.DelegationID)

	seen := make(map[uuid.UUID]struct{})
	var workflowIDs []uuid.UUID
	for _, a := range matched {
		task, err := s.Tasks.GetByID(ctx, in.TenantID, a.TaskID)
		if err != nil {
			continue
		}
		if _, ok := seen[task.WorkflowInstanceID]; ok {
			continue
		}
		seen[task.WorkflowInstanceID] = struct{}{}
		workflowIDs = append(workflowIDs, task.WorkflowInstanceID)
	}

	limit := in.Page.Limit
	if limit <= 0 || limit > len(workflowIDs) {
		limit = len(workflowIDs)
	}
	return port.DelegateImpactResult{
		ReassignedCount: len(matched),
		WorkflowIDs:     port.PageResult[uuid.UUID]{Items: workflowIDs[:limit]},
	}, nil
}

// signalReassignForTask fires the same instance-reassign signal
// TaskService's own admin-driven Reassign uses. AdminUserID is left empty —
// this is a system/delegation-driven bulk action with no specific admin
// actor behind it, and attributing it to the new delegate themselves would
// misrepresent the audit trail.
func (s *WorkflowClient) signalReassignForTask(ctx context.Context, task *domain.Task, oldUserID, newUserID uuid.UUID) error {
	inst, err := s.Instances.GetByID(ctx, task.TenantID, task.WorkflowInstanceID)
	if err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceReassign, reassignSignalWire{
		TaskID: task.ID.String(), OldUserID: oldUserID.String(), NewUserID: newUserID.String(),
		RecordVersion: task.RecordVersion,
	})
}
