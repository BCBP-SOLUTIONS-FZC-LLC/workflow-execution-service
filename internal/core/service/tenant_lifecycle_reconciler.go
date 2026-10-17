package service

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TenantLifecycleReconciler = (*TenantLifecycleReconciler)(nil)

type TenantLifecycleReconciler struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Queues      port.ActiveTaskQueueRepository
	Outbox      port.OutboxRepository
	Transactor  port.Transactor
	Temporal    port.TemporalClient
	Validator   port.EventValidator
	Log         port.Logger
}

func (s *TenantLifecycleReconciler) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func (s *TenantLifecycleReconciler) Apply(ctx context.Context, in port.TenantLifecycleInput) error {
	if in.Status != in.PreviousStatus {
		if err := s.applyStatusTransition(ctx, in); err != nil {
			return err
		}
	}
	if in.Plan != in.PreviousPlan {
		if err := s.applyPlanChange(ctx, in); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantLifecycleReconciler) applyStatusTransition(ctx context.Context, in port.TenantLifecycleInput) error {
	switch in.Status {
	case "offboarded":
		return s.terminateAllNonTerminal(ctx, in.TenantID)
	case "active":
		return s.signalAllNonTerminal(ctx, in.TenantID, port.SignalInstanceResume)
	default:
		return s.signalAllNonTerminal(ctx, in.TenantID, port.SignalInstancePause)
	}
}

var nonTerminalInstanceStatuses = []domain.InstanceStatus{
	domain.InstanceStatusRunning,
	domain.InstanceStatusPaused,
	domain.InstanceStatusDegraded,
}

func allNonTerminalInstances(ctx context.Context, instances port.InstanceRepository, tenantID uuid.UUID) ([]*domain.Instance, error) {
	var out []*domain.Instance
	filter := port.InstanceListFilter{Statuses: nonTerminalInstanceStatuses}
	var after *port.Cursor
	for {
		page, next, err := instances.ListByTenant(ctx, tenantID, filter, port.PageRequest{After: after, Limit: 100})
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == nil {
			break
		}
		after = next
	}
	return out, nil
}

func (s *TenantLifecycleReconciler) signalAllNonTerminal(ctx context.Context, tenantID uuid.UUID, signalName string) error {
	instances, err := allNonTerminalInstances(ctx, s.Instances, tenantID)
	if err != nil {
		return err
	}
	for _, inst := range instances {
		if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, signalName, adminSignalWire{
			Initiator: domain.InitiatorTenantState, RecordVersion: inst.RecordVersion,
		}); err != nil {
			s.logger().Warn("tenant lifecycle: failed to signal instance", map[string]any{"instance_id": inst.ID, "signal": signalName, "error": err.Error()})
		}
	}
	return nil
}

func (s *TenantLifecycleReconciler) terminateAllNonTerminal(ctx context.Context, tenantID uuid.UUID) error {
	toTerminate, err := allNonTerminalInstances(ctx, s.Instances, tenantID)
	if err != nil {
		return err
	}
	var failures []error
	for _, inst := range toTerminate {
		if err := s.terminateOne(ctx, tenantID, inst); err != nil {
			s.logger().Warn("tenant lifecycle: failed to terminate instance during offboard", map[string]any{"instance_id": inst.ID, "error": err.Error()})
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (s *TenantLifecycleReconciler) terminateOne(ctx context.Context, tenantID uuid.UUID, inst *domain.Instance) error {
	if err := s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, "tenant offboarded"); err != nil {
		return err
	}
	var updated *domain.Instance
	return s.Transactor.RunInTxWithRetry(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		current, err := s.Instances.GetByID(ctx, tenantID, inst.ID)
		if err != nil {
			return wrapInstanceErr(err)
		}
		if isTerminalInstanceStatus(current.Status) {
			return nil
		}
		updated, err = s.Instances.UpdateStatus(ctx, tenantID, inst.ID, domain.InstanceStatusTerminated, current.RecordVersion)
		if err != nil {
			return wrapInstanceErr(err)
		}
		if err := cascadeFailTasksForInstance(ctx, s.Tasks, s.Assignments, tenantID, inst.ID); err != nil {
			return err
		}
		payload := domain.NewWorkflowInstanceTerminatedPayload(instanceCore(updated), updated.StartedByUserID, domain.TerminatedInitiatorTenantState, nil)
		sink := instanceEventSink{Outbox: s.Outbox, Validator: s.Validator}
		return sink.enqueueInstanceEvent(ctx, tenantID.String(), domain.EventWorkflowInstanceTerminated, inst.ID.String(), "", payload)
	})
}

func (s *TenantLifecycleReconciler) applyPlanChange(ctx context.Context, in port.TenantLifecycleInput) error {
	queueName := "wf-queue-" + in.TenantID.String()
	if in.Plan == domain.PlanEnterprise {
		_, err := s.Queues.Register(ctx, in.TenantID, queueName)
		return err
	}

	count, err := s.Instances.CountActiveByTaskQueue(ctx, in.TenantID, queueName)
	if err != nil {
		return err
	}
	if count > 0 {
		s.logger().Warn("tenant lifecycle: plan downgrade deferred, isolated queue still has active instances", map[string]any{
			"tenant_id": in.TenantID, "queue_name": queueName, "active_count": count,
		})
		return nil
	}
	return s.Queues.Deregister(ctx, queueName)
}
