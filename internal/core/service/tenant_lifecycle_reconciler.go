package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TenantLifecycleReconciler = (*TenantLifecycleReconciler)(nil)

// TenantLifecycleReconciler implements port.TenantLifecycleReconciler (LLD
// §6.2 item 4): TenantStateChanged's status-transition dispatch (terminate/
// resume/pause) and, independently, its plan-tier isolated-queue
// upsert/cleanup (§3.2, §4.6) — both sub-transactions apply on one event
// when both status and plan actually changed, matching the handler's own
// "commit recency once, after every sub-transaction the event carries"
// contract (internal_events.go's handleTenantStateChanged).
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

// applyStatusTransition dispatches on the resolved status (LLD §6.2 item
// 4.2): offboarded terminates every non-terminal instance; active resumes
// the tenant-state-paused ones; any other value (suspended/cancelled/
// past_due/trial_expired today) pauses every RUNNING instance — a single
// default branch, not a hardcoded enumeration, so a future status value
// needs no code change here.
func (s *TenantLifecycleReconciler) applyStatusTransition(ctx context.Context, in port.TenantLifecycleInput) error {
	switch in.Status {
	case "offboarded":
		return s.terminateAllNonTerminal(ctx, in.TenantID)
	case "active":
		return s.signalAllByStatus(ctx, in.TenantID, domain.InstanceStatusPaused, port.SignalInstanceResume)
	default:
		return s.signalAllByStatus(ctx, in.TenantID, domain.InstanceStatusRunning, port.SignalInstancePause)
	}
}

// allInstancesByStatus pages through every one of tenantID's instances
// currently in status want — a tenant-wide sweep, unlike
// OOOAvailabilityReconciler's per-user-assignment establish step.
func allInstancesByStatus(ctx context.Context, instances port.InstanceRepository, tenantID uuid.UUID, want domain.InstanceStatus) ([]*domain.Instance, error) {
	var out []*domain.Instance
	filter := port.InstanceListFilter{Status: &want}
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

// signalAllByStatus is the tenant-wide pause/resume sweep (LLD §6.2 item 4
// dispatch branches 2-3). Same best-effort "every instance currently in the
// given status, not filtered by which initiator actually paused it"
// simplification OOOAvailabilityReconciler documents — neither
// PauseInstanceInput nor ResumeInstanceInput persists an initiator today.
func (s *TenantLifecycleReconciler) signalAllByStatus(ctx context.Context, tenantID uuid.UUID, want domain.InstanceStatus, signalName string) error {
	instances, err := allInstancesByStatus(ctx, s.Instances, tenantID, want)
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
	var toTerminate []*domain.Instance
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded} {
		rows, err := allInstancesByStatus(ctx, s.Instances, tenantID, status)
		if err != nil {
			return err
		}
		toTerminate = append(toTerminate, rows...)
	}
	for _, inst := range toTerminate {
		if err := s.terminateOne(ctx, tenantID, inst); err != nil {
			s.logger().Warn("tenant lifecycle: failed to terminate instance during offboard", map[string]any{"instance_id": inst.ID, "error": err.Error()})
		}
	}
	return nil
}

// terminateOne mirrors InstanceService.Terminate's own cascade+commit shape
// (shared via cascadeFailTasksForInstance/instanceEventSink) with two
// differences the tenant-offboard trigger requires: TerminatedInitiatorTenantState
// instead of Admin, and no actorUserID — this is a system-driven sweep, not
// a specific admin's action.
func (s *TenantLifecycleReconciler) terminateOne(ctx context.Context, tenantID uuid.UUID, inst *domain.Instance) error {
	var updated *domain.Instance
	txErr := s.Transactor.RunInTxWithRetry(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		var err error
		updated, err = s.Instances.UpdateStatus(ctx, tenantID, inst.ID, domain.InstanceStatusTerminated, inst.RecordVersion)
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
	if txErr != nil {
		return txErr
	}
	return s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, "tenant offboarded")
}

// applyPlanChange is the isolated-queue upsert/cleanup half of LLD §6.2 item
// 4 (§3.2, §4.6): an enterprise plan gets wf-queue-<tenant_uuid> registered;
// any other plan attempts to deregister it, but only once no instance
// started on it is still active — a tenant that downgrades while instances
// are still running on its isolated queue stays registered past this event,
// with no automatic retry sweep once those instances finish (a future task,
// same spirit as this package's other documented best-effort simplifications).
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
