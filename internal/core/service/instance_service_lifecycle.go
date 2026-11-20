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

type adminSignalWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
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
	if isTerminalInstanceStatus(inst.Status) {
		return port.ErrInstanceAlreadyTerminal
	}
	if inst.RecordVersion != recordVersion {
		return staleInstanceVersion()
	}
	if err := validateInstanceStateFor(signalName, inst.Status); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, signalName, adminSignalWire{
		AdminUserID: actorUserID.String(), Reason: ann.reason, Initiator: ann.initiator, RecordVersion: recordVersion,
	})
}

func staleInstanceVersion() error {
	observability.IncCounterVec(observability.RecordVersionConflictTotal, "workflow_instance")
	return port.ErrRecordVersionConflict
}

func validateInstanceStateFor(signalName string, status domain.InstanceStatus) error {
	var allowed []domain.InstanceStatus
	switch signalName {
	case port.SignalInstancePause:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded}
	case port.SignalInstanceResume:
		allowed = []domain.InstanceStatus{domain.InstanceStatusPaused, domain.InstanceStatusDegraded}
	case port.SignalInstanceCancel:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded}
	case port.SignalInstanceForceFwd, port.SignalInstanceForceBack, port.SignalInstanceExternalAnswer:
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
	err := s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{reason: reason, initiator: domain.InitiatorAdmin}, recordVersion, port.SignalInstancePause)
	return s.endIfWorkflowGone(ctx, err, tenantID, instanceID, actorUserID, workflowGoneReason)
}

func (s *InstanceService) Resume(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
	err := s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{initiator: domain.InitiatorAdmin}, recordVersion, port.SignalInstanceResume)
	return s.endIfWorkflowGone(ctx, err, tenantID, instanceID, actorUserID, workflowGoneReason)
}

func (s *InstanceService) Cancel(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	err := s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{reason: reason}, recordVersion, port.SignalInstanceCancel)
	return s.endIfWorkflowGone(ctx, err, tenantID, instanceID, actorUserID, reason)
}

const workflowGoneReason = "the workflow execution no longer exists"

func (s *InstanceService) endIfWorkflowGone(ctx context.Context, signalErr error, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
	if !errors.Is(signalErr, port.ErrWorkflowNotFound) {
		return signalErr
	}
	s.logger().Warn("a lifecycle signal found no workflow execution, ending the instance row", map[string]any{
		"instance_id": instanceID, "tenant_id": tenantID,
	})
	_, err := s.markTerminated(ctx, tenantID, instanceID, actorUserID, reason)
	return err
}

func (s *InstanceService) ForceForward(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if isTerminalInstanceStatus(inst.Status) {
		return port.ErrInstanceAlreadyTerminal
	}
	if inst.RecordVersion != recordVersion {
		return staleInstanceVersion()
	}
	if err := validateInstanceStateFor(port.SignalInstanceForceFwd, inst.Status); err != nil {
		return err
	}
	if err := s.validateForwardTarget(ctx, tenantID, inst, targetNodeKey); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceForceFwd, adminSignalWire{
		AdminUserID: actorUserID.String(), TargetNodeKey: targetNodeKey, RecordVersion: recordVersion,
	})
}

func (s *InstanceService) ForceBack(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if isTerminalInstanceStatus(inst.Status) {
		return port.ErrInstanceAlreadyTerminal
	}
	if inst.RecordVersion != recordVersion {
		return staleInstanceVersion()
	}
	if err := validateInstanceStateFor(port.SignalInstanceForceBack, inst.Status); err != nil {
		return err
	}
	if inst.Status == domain.InstanceStatusRunning {
		done, err := hasCompletedStep(ctx, s.Tasks, tenantID, inst.ID)
		if err != nil {
			return err
		}
		if !done {
			return port.ErrForceBackNoSavedBranch
		}
	}
	if inst.Status == domain.InstanceStatusRunning {
		targetNodeKey = ""
	}
	if err := s.validateBackTarget(ctx, tenantID, inst, targetNodeKey); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceForceBack, adminSignalWire{
		AdminUserID: actorUserID.String(), TargetNodeKey: targetNodeKey, RecordVersion: recordVersion,
	})
}

func (s *InstanceService) Terminate(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if isTerminalInstanceStatus(inst.Status) {
		return port.ErrInstanceAlreadyTerminal
	}

	if err := s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, inst.TemporalRunID, reason); err != nil {
		s.logger().Error("TerminateWorkflow failed, instance left as it was", map[string]any{
			"instance_id": instanceID, "tenant_id": tenantID, "error": err.Error(),
		})
		return fmt.Errorf("terminate workflow: %w", err)
	}

	inst, txErr := s.markTerminated(ctx, tenantID, instanceID, actorUserID, reason)
	if txErr != nil {
		return txErr
	}

	if err := s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, inst.TemporalRunID, reason); err != nil {
		s.logger().Error("TerminateWorkflow failed after the instance was marked terminated", map[string]any{
			"instance_id": instanceID, "tenant_id": tenantID, "error": err.Error(),
		})
	}
	return nil
}

// terminateAttempts bounds how often a terminate transaction is repeated after a concurrent
// change: the request carries no record version and the run is already ended, so a bump by
// the workflow's last write must not leave the row running.
const terminateAttempts = 3

func (s *InstanceService) markTerminated(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) (*domain.Instance, error) {
	var err error
	for attempt := 0; attempt < terminateAttempts; attempt++ {
		var inst *domain.Instance
		inst, err = s.terminateOnce(ctx, tenantID, instanceID, actorUserID, reason)
		if !errors.Is(err, port.ErrRecordVersionConflict) {
			return inst, err
		}
	}
	return nil, err
}

func (s *InstanceService) terminateOnce(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) (*domain.Instance, error) {
	var inst *domain.Instance
	err := s.Transactor.RunInTx(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		current, err := s.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return wrapInstanceErr(err)
		}
		if isTerminalInstanceStatus(current.Status) {
			return port.ErrInstanceAlreadyTerminal
		}
		updated, err := s.Instances.UpdateStatus(ctx, tenantID, instanceID, domain.InstanceStatusTerminated, current.RecordVersion)
		if err != nil {
			return wrapInstanceErr(err)
		}
		inst = updated
		sink := instanceEventSink{Outbox: s.Outbox, Validator: s.Validator}
		cascade := taskCascade{Tasks: s.Tasks, Assignments: s.Assignments, Sink: sink}
		if err := cascade.failTasks(ctx, tenantID, inst); err != nil {
			return err
		}
		return sink.enqueueInstanceEvent(ctx, tenantID.String(), domain.EventWorkflowInstanceTerminated, instanceID.String(),
			actorUserID.String(), domain.NewWorkflowInstanceTerminatedPayload(instanceCore(inst), inst.StartedByUserID, domain.TerminatedInitiatorAdmin, &actorUserID, &reason))
	})
	return inst, err
}

type taskCascade struct {
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Sink        instanceEventSink
}

func (c taskCascade) failTasks(ctx context.Context, tenantID uuid.UUID, inst *domain.Instance) error {
	var after *port.Cursor
	for {
		rows, next, err := c.Tasks.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{After: after, Limit: 100})
		if err != nil {
			return wrapTaskErr(err)
		}
		for _, t := range rows {
			if err := c.failOpenTask(ctx, tenantID, inst, t); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		after = next
	}
}

func (c taskCascade) failOpenTask(ctx context.Context, tenantID uuid.UUID, inst *domain.Instance, t *domain.Task) error {
	if t.Status != domain.TaskStatusReady && t.Status != domain.TaskStatusInProgress {
		return nil
	}
	if _, err := c.Tasks.UpdateStatus(ctx, tenantID, t.ID, domain.TaskStatusFailed, t.RecordVersion); err != nil {
		return wrapTaskErr(err)
	}
	active, err := c.Assignments.ListActiveByTask(ctx, tenantID, t.ID)
	if err != nil {
		return err
	}
	assigneeUserIDs := make([]uuid.UUID, 0, len(active))
	for _, a := range active {
		if _, err := c.Assignments.Vacate(ctx, tenantID, a.ID); err != nil {
			return err
		}
		assigneeUserIDs = append(assigneeUserIDs, a.UserID)
	}
	taskCore := domain.TaskScopedCore{TaskID: t.ID, NodeKey: t.NodeKey, DepartmentID: t.DepartmentID, AssigneeUserIDs: assigneeUserIDs}
	payload := domain.NewWorkflowTaskFailedPayload(instanceCore(inst), taskCore, domain.CascadeSourceInstanceTerminate, "")
	return c.Sink.enqueueInstanceEvent(ctx, tenantID.String(), domain.EventWorkflowTaskFailed, inst.ID.String(), "", payload)
}

func isTerminalInstanceStatus(status domain.InstanceStatus) bool {
	return status == domain.InstanceStatusCompleted || status == domain.InstanceStatusTerminated || status == domain.InstanceStatusFailed
}
