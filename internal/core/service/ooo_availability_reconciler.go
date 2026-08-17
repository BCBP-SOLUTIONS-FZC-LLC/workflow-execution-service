package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.OOOAvailabilityReconciler = (*OOOAvailabilityReconciler)(nil)

// OOOAvailabilityReconciler implements port.OOOAvailabilityReconciler (LLD
// §6.2 item 6): status=ooo pauses the user's RUNNING instances
// (initiator=ooo); status=available resumes them. Never a reroute driver —
// DelegateUserID is informational only; an actual reroute only ever happens
// via the paired DelegationStarted event, whose own DelegationReconciler
// also resumes any OOO-paused instances it touches.
//
// Resume is a best-effort approximation of the LLD's own "initiator=ooo
// filtered" resume: PauseInstanceInput/ResumeInstanceInput
// (internal/core/port/activities.go, internal/workflow's own Activity
// callers) carry no initiator/reason field today, so nothing persists which
// admin/tenant_state/ooo/safety_net signal actually paused a given instance
// — every currently-PAUSED instance among the user's active assignments is
// resumed. A future task threading Reason through those Activity inputs
// would let this filter precisely instead.
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
		return s.signalActiveInstances(ctx, in.TenantID, in.UserID, domain.InstanceStatusRunning, port.SignalInstancePause)
	case "available":
		return s.signalActiveInstances(ctx, in.TenantID, in.UserID, domain.InstanceStatusPaused, port.SignalInstanceResume)
	default:
		return nil
	}
}

// signalActiveInstances establishes the distinct instances backing userID's
// active assignments and signals every one currently in status want. A
// non-matching status, an unreadable task/instance, or a per-instance
// signal failure is logged and skipped, never aborts the batch (the same
// bulk-signal-loop exception UserTaskPauser documents for its own callers).
func (s *OOOAvailabilityReconciler) signalActiveInstances(ctx context.Context, tenantID, userID uuid.UUID, want domain.InstanceStatus, signalName string) error {
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
		if inst.Status != want {
			continue
		}
		if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, signalName, adminSignalWire{
			Reason: domain.InitiatorOOO, RecordVersion: inst.RecordVersion,
		}); err != nil {
			s.logger().Warn("ooo availability: failed to signal instance", map[string]any{"instance_id": inst.ID, "signal": signalName, "error": err.Error()})
		}
	}
	return nil
}
