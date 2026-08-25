package service

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.DelegationReconciler = (*DelegationReconciler)(nil)

type DelegationReconciler struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Outbox      port.OutboxRepository
	Transactor  port.Transactor
	Temporal    port.TemporalClient
	Definitions port.DefinitionServiceClient
	Eligibility port.EligibilityChecker
	Validator   port.EventValidator
	Log         port.Logger
}

func (s *DelegationReconciler) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

type delegationCandidate struct {
	assignment *domain.TaskAssignment
	task       *domain.Task
	instance   *domain.Instance
}

func (s *DelegationReconciler) resolveCandidates(ctx context.Context, tenantID uuid.UUID, assignments []*domain.TaskAssignment) []delegationCandidate {
	out := make([]delegationCandidate, 0, len(assignments))
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			s.logger().Warn("delegation reconciler: skipping assignment with unreadable task", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			s.logger().Warn("delegation reconciler: skipping assignment with unreadable instance", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		out = append(out, delegationCandidate{assignment: a, task: task, instance: inst})
	}
	return out
}

func (s *DelegationReconciler) eligibleCandidates(ctx context.Context, tenantID, candidateUserID uuid.UUID, rows []delegationCandidate, logPrefix string) ([]delegationCandidate, error) {
	plans := newCompiledPlanCache(s.Definitions)
	requests := make([]port.EligibilityCheckRequest, 0, len(rows))
	resolved := make([]delegationCandidate, 0, len(rows))
	for _, c := range rows {
		plan, err := plans.mainPlan(ctx, tenantID, c.instance.WorkflowVersionID)
		if err != nil {
			s.logger().Warn(logPrefix+": skipping row with unreadable compiled plan", map[string]any{"task_id": c.task.ID, "error": err.Error()})
			continue
		}
		level, ok := requiredLevelForTask(plan, c.task)
		if !ok {
			s.logger().Warn(logPrefix+": skipping row with no matching stage in compiled plan", map[string]any{"task_id": c.task.ID})
			continue
		}
		requests = append(requests, port.EligibilityCheckRequest{NewUserID: candidateUserID, DepartmentID: c.task.DepartmentID, RequiredLevel: level})
		resolved = append(resolved, c)
	}
	if len(resolved) == 0 {
		return nil, nil
	}

	eligible, err := s.Eligibility.CheckEligibilityBatch(ctx, requests, candidateUserID)
	if err != nil {
		return nil, err
	}
	out := make([]delegationCandidate, 0, len(resolved))
	for i, ok := range eligible {
		if ok {
			out = append(out, resolved[i])
			continue
		}
		s.logger().Warn(logPrefix+": row held, candidate ineligible for node", map[string]any{"task_id": resolved[i].task.ID})
	}
	return out, nil
}

func (s *DelegationReconciler) enqueueTaskReassigned(ctx context.Context, c delegationCandidate, oldUserID, newUserID uuid.UUID, delegationID *uuid.UUID) error {
	core := domain.CommonCore{WorkflowInstanceID: c.instance.ID, BusinessKey: c.instance.BusinessKey, WorkflowVersionID: c.instance.WorkflowVersionID}
	taskCore := domain.TaskScopedCore{TaskID: c.task.ID, NodeKey: c.task.NodeKey, DepartmentID: c.task.DepartmentID, AssigneeUserIDs: []uuid.UUID{newUserID}}
	payload := domain.NewWorkflowTaskReassignedPayload(core, taskCore, oldUserID, newUserID, domain.ReassignInitiatorDelegation, delegationID)
	subject := "instances/" + c.instance.ID.String() + "/tasks/" + c.task.ID.String()
	envelope, err := BuildEnvelope(ctx, s.Validator, domain.EventWorkflowTaskReassigned, c.instance.TenantID.String(), subject, "", payload)
	if err != nil {
		return err
	}
	return s.Outbox.Enqueue(ctx, envelope)
}

func (s *DelegationReconciler) commitReassignments(ctx context.Context, tenantID uuid.UUID, rows []delegationCandidate, newUserID uuid.UUID, reason string, delegationID *uuid.UUID) error {
	txErr := s.Transactor.RunInTx(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		for _, c := range rows {
			if _, err := s.Assignments.Vacate(ctx, tenantID, c.assignment.ID); err != nil {
				return err
			}
			newAssignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: c.task.ID, UserID: newUserID, Reason: reason}
			if err := s.Assignments.Create(ctx, newAssignment); err != nil {
				return err
			}
			if err := s.enqueueTaskReassigned(ctx, c, c.assignment.UserID, newUserID, delegationID); err != nil {
				return err
			}
		}
		return nil
	})
	if txErr != nil {
		return txErr
	}

	for _, c := range rows {
		if err := s.Temporal.SignalWorkflow(ctx, c.instance.TemporalWorkflowID, c.instance.ID, port.SignalInstanceReassign, reassignSignalWire{
			TaskID: c.task.ID.String(), OldUserID: c.assignment.UserID.String(), NewUserID: newUserID.String(), RecordVersion: c.task.RecordVersion,
		}); err != nil {
			s.logger().Warn("delegation reconciler: failed to signal instance-reassign, DB state already updated", map[string]any{"task_id": c.task.ID, "error": err.Error()})
		}
	}
	return nil
}

func (s *DelegationReconciler) Reroute(ctx context.Context, in port.DelegationRerouteInput) error {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegatorID)
	if err != nil {
		return err
	}
	candidates := s.resolveCandidates(ctx, in.TenantID, active)

	plans := newCompiledPlanCache(s.Definitions)
	scoped := make([]delegationCandidate, 0, len(candidates))
	for _, c := range candidates {
		if scopeMatches(ctx, plans, in.TenantID, c, in.Scope, in.ScopeID) {
			scoped = append(scoped, c)
		}
	}
	if len(scoped) == 0 {
		return nil
	}

	eligible, err := s.eligibleCandidates(ctx, in.TenantID, in.DelegateID, scoped, "delegation reroute")
	if err != nil {
		return err
	}
	if len(eligible) == 0 {
		return nil
	}

	return s.commitReassignments(ctx, in.TenantID, eligible, in.DelegateID, delegationReasonTag(in.DelegationID), &in.DelegationID)
}

func (s *DelegationReconciler) Reverse(ctx context.Context, in port.DelegationReversalInput) error {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateID)
	if err != nil {
		return err
	}
	candidates := s.resolveCandidates(ctx, in.TenantID, active)

	tag := delegationReasonTag(in.DelegationID)
	tagged := make([]delegationCandidate, 0, len(candidates))
	for _, c := range candidates {
		if strings.HasPrefix(c.assignment.Reason, tag) {
			tagged = append(tagged, c)
		}
	}
	if len(tagged) == 0 {
		return nil
	}

	eligible, err := s.eligibleCandidates(ctx, in.TenantID, in.DelegatorID, tagged, "delegation reversal")
	if err != nil {
		return err
	}
	if len(eligible) == 0 {
		return nil
	}

	return s.commitReassignments(ctx, in.TenantID, eligible, in.DelegatorID, "", &in.DelegationID)
}
