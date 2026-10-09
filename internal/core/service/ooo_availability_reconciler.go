package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.OOOAvailabilityReconciler = (*OOOAvailabilityReconciler)(nil)

type OOOAvailabilityReconciler struct {
	Instances   port.InstanceRepository
	Assignments port.TaskAssignmentRepository
	Tasks       port.TaskRepository
	Temporal    port.TemporalClient
	Log         port.Logger
}

func (s *OOOAvailabilityReconciler) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func (s *OOOAvailabilityReconciler) Apply(ctx context.Context, in port.UserAvailabilityInput) error {
	switch in.Status {
	case "ooo":
		if in.DelegateUserID != nil {
			return nil
		}
		return s.signalActiveInstances(ctx, in.TenantID, in.UserID, port.SignalInstancePause)
	case "available":
		return s.signalActiveInstances(ctx, in.TenantID, in.UserID, port.SignalInstanceResume)
	default:
		return nil
	}
}

func (s *OOOAvailabilityReconciler) signalActiveInstances(ctx context.Context, tenantID, userID uuid.UUID, signalName string) error {
	assignments, err := s.Assignments.ListActiveByUser(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	seen := make(map[uuid.UUID]struct{})
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			s.logger().Warn("ooo availability: skipping assignment with unreadable task", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		if _, ok := seen[task.WorkflowInstanceID]; ok {
			continue
		}
		seen[task.WorkflowInstanceID] = struct{}{}

		inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			s.logger().Warn("ooo availability: skipping unreadable instance", map[string]any{"instance_id": task.WorkflowInstanceID, "error": err.Error()})
			continue
		}
		if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, signalName, adminSignalWire{
			Initiator: domain.InitiatorOOO, RecordVersion: inst.RecordVersion,
		}); err != nil {
			s.logger().Warn("ooo availability: failed to signal instance", map[string]any{"instance_id": inst.ID, "signal": signalName, "error": err.Error()})
		}
	}
	return nil
}
