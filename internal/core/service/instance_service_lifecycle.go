package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type adminSignalWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetDeptID  string
	TargetNodeKey string
	RecordVersion int64
}

type signalAnnotation struct {
	reason    string
	initiator string
}

func (s *InstanceService) signalLifecycle(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, ann signalAnnotation, recordVersion int64, signalName string) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if inst.RecordVersion != recordVersion {
		return port.ErrRecordVersionConflict
	}
	if err := validateInstanceStateFor(signalName, inst.Status); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, signalName, adminSignalWire{
		AdminUserID: actorUserID.String(), Reason: ann.reason, Initiator: ann.initiator, RecordVersion: recordVersion,
	})
}

func validateInstanceStateFor(signalName string, status domain.InstanceStatus) error {
	var allowed []domain.InstanceStatus
	switch signalName {
	case port.SignalInstancePause:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning}
	case port.SignalInstanceResume:
		allowed = []domain.InstanceStatus{domain.InstanceStatusPaused}
	case port.SignalInstanceCancel:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded}
	case port.SignalInstanceForceFwd, port.SignalInstanceForceBack:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded}
	}
	for _, a := range allowed {
		if a == status {
			return nil
		}
	}
	if status == domain.InstanceStatusCompleted || status == domain.InstanceStatusTerminated || status == domain.InstanceStatusFailed {
		return port.ErrInstanceAlreadyTerminal
	}
	return port.ErrInvalidInstanceState
}

func (s *InstanceService) Pause(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{reason: reason, initiator: domain.InitiatorAdmin}, recordVersion, port.SignalInstancePause)
}

func (s *InstanceService) Resume(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{initiator: domain.InitiatorAdmin}, recordVersion, port.SignalInstanceResume)
}

func (s *InstanceService) Cancel(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{reason: reason}, recordVersion, port.SignalInstanceCancel)
}

func (s *InstanceService) ForceForward(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if inst.RecordVersion != recordVersion {
		return port.ErrRecordVersionConflict
	}
	if err := validateInstanceStateFor(port.SignalInstanceForceFwd, inst.Status); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceForceFwd, adminSignalWire{
		AdminUserID: actorUserID.String(), TargetNodeKey: targetNodeKey, RecordVersion: recordVersion,
	})
}

func (s *InstanceService) ForceBack(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{}, recordVersion, port.SignalInstanceForceBack)
}

func (s *InstanceService) Terminate(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if inst.Status == domain.InstanceStatusCompleted || inst.Status == domain.InstanceStatusTerminated || inst.Status == domain.InstanceStatusFailed {
		return port.ErrInstanceAlreadyTerminal
	}

	txErr := s.Transactor.RunInTxWithRetry(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		updated, err := s.Instances.UpdateStatus(ctx, tenantID, instanceID, domain.InstanceStatusTerminated, inst.RecordVersion)
		if err != nil {
			return wrapInstanceErr(err)
		}
		if err := cascadeFailTasksForInstance(ctx, s.Tasks, s.Assignments, tenantID, instanceID); err != nil {
			return err
		}
		inst = updated
		sink := instanceEventSink{Outbox: s.Outbox, Validator: s.Validator}
		return sink.enqueueInstanceEvent(ctx, tenantID.String(), domain.EventWorkflowInstanceTerminated, instanceID.String(),
			actorUserID.String(), domain.NewWorkflowInstanceTerminatedPayload(instanceCore(inst), inst.StartedByUserID, domain.TerminatedInitiatorAdmin, &actorUserID))
	})
	if txErr != nil {
		return txErr
	}

	if err := s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, reason); err != nil {
		s.logger().Error("TerminateWorkflow failed after DB state already committed TERMINATED", map[string]any{
			"instance_id": instanceID, "tenant_id": tenantID, "error": err.Error(),
		})
		return fmt.Errorf("terminate workflow: %w", err)
	}
	return nil
}

func cascadeFailTasksForInstance(ctx context.Context, tasks port.TaskRepository, assignments port.TaskAssignmentRepository, tenantID, instanceID uuid.UUID) error {
	rows, _, err := tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
	if err != nil {
		return wrapTaskErr(err)
	}
	for _, t := range rows {
		if t.Status != domain.TaskStatusReady && t.Status != domain.TaskStatusInProgress {
			continue
		}
		if _, err := tasks.UpdateStatus(ctx, tenantID, t.ID, domain.TaskStatusFailed, t.RecordVersion); err != nil {
			return wrapTaskErr(err)
		}
		active, err := assignments.ListActiveByTask(ctx, tenantID, t.ID)
		if err != nil {
			return err
		}
		for _, a := range active {
			if _, err := assignments.Vacate(ctx, tenantID, a.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
