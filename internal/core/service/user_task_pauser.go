package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.UserTaskPauser = (*UserTaskPauser)(nil)

// UserTaskPauser implements port.UserTaskPauser — the inbound gRPC
// PauseUserTasks RPC, the DepartmentMembershipRevoked/UserDeleted safety-net
// pause vehicle (LLD §3.1). Bulk-signal-loop exception: a per-instance
// version mismatch or non-pausable status is logged and skipped, never
// aborts the whole batch, since this iterates every instance a single event
// affects with no per-instance human review.
type UserTaskPauser struct {
	Instances   port.InstanceRepository
	Assignments port.TaskAssignmentRepository
	Tasks       port.TaskRepository
	Temporal    port.TemporalClient
	Log         port.Logger
}

func (s *UserTaskPauser) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func (s *UserTaskPauser) PauseUserTasks(ctx context.Context, tenantID, userID uuid.UUID) error {
	assignments, err := s.Assignments.ListActiveByUser(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	seen := make(map[uuid.UUID]struct{})
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			s.logger().Warn("skipping assignment with unreadable task during safety-net pause", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		if _, ok := seen[task.WorkflowInstanceID]; ok {
			continue
		}
		seen[task.WorkflowInstanceID] = struct{}{}

		inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			s.logger().Warn("skipping unreadable instance during safety-net pause", map[string]any{"instance_id": task.WorkflowInstanceID, "error": err.Error()})
			continue
		}
		if inst.Status != domain.InstanceStatusRunning {
			s.logger().Warn("skipping non-pausable instance during safety-net pause", map[string]any{"instance_id": inst.ID, "status": inst.Status})
			continue
		}
		if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstancePause, adminSignalWire{
			Initiator: domain.InitiatorSafetyNet, RecordVersion: inst.RecordVersion,
		}); err != nil {
			s.logger().Warn("failed to signal instance-pause during safety-net pause", map[string]any{"instance_id": inst.ID, "error": err.Error()})
		}
	}
	return nil
}
