package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

var _ port.UserTaskPauser = (*UserTaskPauser)(nil)

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

// PauseUserTasks pauses every running instance the user holds an active task on.
// It attempts every instance and returns the joined failures, so the caller
// (Definition, which retries a 5xx) does not acknowledge a revocation that left
// an instance running. An instance whose workflow is already gone counts as paused.
func (s *UserTaskPauser) PauseUserTasks(ctx context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error {
	assignments, err := s.Assignments.ListActiveByUser(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	var failures []error
	fail := func(stage, msg string, fields map[string]any, err error) {
		observability.IncCounterVec(observability.UserPauseFailuresTotal, stage)
		fields["error"] = err.Error()
		s.logger().Warn(msg, fields)
		failures = append(failures, fmt.Errorf("%s: %w", msg, err))
	}

	seen := make(map[uuid.UUID]struct{})
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			fail("read_task", "cannot read task during safety-net pause", map[string]any{"assignment_id": a.ID}, err)
			continue
		}
		if departmentID != nil && task.DepartmentID != *departmentID {
			continue
		}
		if _, ok := seen[task.WorkflowInstanceID]; ok {
			continue
		}
		seen[task.WorkflowInstanceID] = struct{}{}

		inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			fail("read_instance", "cannot read instance during safety-net pause", map[string]any{"instance_id": task.WorkflowInstanceID}, err)
			continue
		}
		if inst.Status != domain.InstanceStatusRunning {
			s.logger().Warn("skipping non-pausable instance during safety-net pause", map[string]any{"instance_id": inst.ID, "status": inst.Status})
			continue
		}
		if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstancePause, adminSignalWire{
			Initiator: domain.InitiatorSafetyNet, RecordVersion: inst.RecordVersion,
		}); errors.Is(err, port.ErrWorkflowNotFound) {
			// The workflow has completed or never existed (Temporal's NotFound): nothing is left running to pause.
			s.logger().Info("instance workflow already gone during safety-net pause", map[string]any{"instance_id": inst.ID})
		} else if err != nil {
			fail("signal", "cannot signal instance-pause during safety-net pause", map[string]any{"instance_id": inst.ID}, err)
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%w: %w", port.ErrUserPauseIncomplete, errors.Join(failures...))
	}
	return nil
}
