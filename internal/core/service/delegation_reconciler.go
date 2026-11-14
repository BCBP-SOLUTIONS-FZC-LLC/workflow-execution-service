package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.DelegationReconciler = (*DelegationReconciler)(nil)

type DelegationReconciler struct {
	Instances    port.InstanceRepository
	Tasks        port.TaskRepository
	Assignments  port.TaskAssignmentRepository
	Outbox       port.OutboxRepository
	Transactor   port.Transactor
	Temporal     port.TemporalClient
	Definitions  port.DefinitionServiceClient
	Eligibility  port.EligibilityChecker
	Validator    port.EventValidator
	Availability port.UserAvailabilityRepository
	Locks        port.ScopeLocker
	Log          port.Logger
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

// resolveCandidates reads the task and instance of every assignment. A read
// that fails fails the call, so a row is never skipped and acknowledged
// because it could not be read (execution LLD §5.8).
func (s *DelegationReconciler) resolveCandidates(ctx context.Context, tenantID uuid.UUID, assignments []*domain.TaskAssignment) ([]delegationCandidate, error) {
	out := make([]delegationCandidate, 0, len(assignments))
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delegation reconciler: read task of assignment %s: %w", a.ID, err)
		}
		inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			return nil, fmt.Errorf("delegation reconciler: read instance of task %s: %w", task.ID, err)
		}
		out = append(out, delegationCandidate{assignment: a, task: task, instance: inst})
	}
	return out, nil
}

// requirePlans reads the compiled plan of every candidate's instance before
// the scope is matched, because scope matching has no way to report a failed read.
func (s *DelegationReconciler) requirePlans(ctx context.Context, plans *compiledPlanCache, tenantID uuid.UUID, rows []delegationCandidate) error {
	read := make(map[uuid.UUID]struct{}, len(rows))
	for _, c := range rows {
		if _, ok := read[c.instance.WorkflowVersionID]; ok {
			continue
		}
		read[c.instance.WorkflowVersionID] = struct{}{}
		if _, err := plans.mainPlan(ctx, tenantID, c.instance.WorkflowVersionID); err != nil {
			return fmt.Errorf("delegation reconciler: read compiled plan of task %s: %w", c.task.ID, err)
		}
	}
	return nil
}

var errEligibilityAnswerCount = errors.New("eligibility answer count differs from request count")

type eligibilityPair struct {
	department uuid.UUID
	level      string
}

func (s *DelegationReconciler) eligibleCandidates(ctx context.Context, plans *compiledPlanCache, tenantID, candidateUserID uuid.UUID, rows []delegationCandidate, logPrefix string) ([]delegationCandidate, error) {
	pairOf := make([]eligibilityPair, 0, len(rows))
	resolved := make([]delegationCandidate, 0, len(rows))
	index := map[eligibilityPair]int{}
	var requests []port.EligibilityCheckRequest
	for _, c := range rows {
		plan, err := plans.mainPlan(ctx, tenantID, c.instance.WorkflowVersionID)
		if err != nil {
			return nil, fmt.Errorf("%s: read compiled plan of task %s: %w", logPrefix, c.task.ID, err)
		}
		level, ok := requiredLevelForTask(plan, c.task)
		if !ok {
			s.logger().Warn(logPrefix+": skipping row with no matching stage in compiled plan", map[string]any{"task_id": c.task.ID})
			continue
		}
		pair := eligibilityPair{department: c.task.DepartmentID, level: level}
		if _, seen := index[pair]; !seen {
			index[pair] = len(requests)
			requests = append(requests, port.EligibilityCheckRequest{NewUserID: candidateUserID, DepartmentID: pair.department, RequiredLevel: pair.level})
		}
		pairOf = append(pairOf, pair)
		resolved = append(resolved, c)
	}
	if len(resolved) == 0 {
		return nil, nil
	}

	answers, err := s.Eligibility.CheckEligibilityBatch(ctx, tenantID, requests)
	if err != nil {
		return nil, err
	}
	if len(answers) != len(requests) {
		s.logger().Error(logPrefix+": eligibility answer count differs from request count", map[string]any{"requests": len(requests), "answers": len(answers)})
		return nil, fmt.Errorf("%w: %s: %d answers for %d requests", errEligibilityAnswerCount, logPrefix, len(answers), len(requests))
	}
	for _, result := range answers {
		if result.Err != nil {
			return nil, fmt.Errorf("%s: eligibility read: %w", logPrefix, result.Err)
		}
	}
	out := make([]delegationCandidate, 0, len(resolved))
	for i, c := range resolved {
		if answers[index[pairOf[i]]].Eligible {
			out = append(out, c)
			continue
		}
		s.logger().Warn(logPrefix+": row held, candidate ineligible for node", map[string]any{"task_id": c.task.ID})
	}
	return out, nil
}

func (s *DelegationReconciler) enqueueTaskReassigned(ctx context.Context, c delegationCandidate, oldUserID, newUserID uuid.UUID, delegationID *uuid.UUID) error {
	core := domain.CommonCore{WorkflowInstanceID: c.instance.ID, SubjectID: c.instance.SubjectID, BusinessKey: c.instance.BusinessKey, WorkflowVersionID: c.instance.WorkflowVersionID}
	taskCore := domain.TaskScopedCore{TaskID: c.task.ID, NodeKey: c.task.NodeKey, DepartmentID: c.task.DepartmentID, AssigneeUserIDs: []uuid.UUID{newUserID}}
	payload := domain.NewWorkflowTaskReassignedPayload(core, taskCore, oldUserID, newUserID, domain.ReassignInitiatorDelegation, delegationID)
	subject := "instances/" + c.instance.ID.String() + "/tasks/" + c.task.ID.String()
	envelope, err := BuildEnvelope(ctx, s.Validator, domain.EventWorkflowTaskReassigned, c.instance.TenantID.String(), subject, "", payload)
	if err != nil {
		return err
	}
	return s.Outbox.Enqueue(ctx, envelope)
}

// commitReassignments moves the rows in one transaction and returns nothing
// for a row whose assignment ended after it was read: that row gets no new
// assignment, no event and no signal.
func (s *DelegationReconciler) commitReassignments(ctx context.Context, tenantID uuid.UUID, rows []delegationCandidate, newUserID uuid.UUID, reason string, delegationID *uuid.UUID) error {
	var moved []delegationCandidate
	txErr := s.Transactor.RunInTx(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		moved = moved[:0]
		for _, c := range rows {
			active, ok, err := vacateIfActive(ctx, s.Assignments, tenantID, c.assignment)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if err := handOver(ctx, s.Assignments, tenantID, c.assignment, active, c.task, newUserID, reason); err != nil {
				return err
			}
			if err := s.enqueueTaskReassigned(ctx, c, c.assignment.UserID, newUserID, delegationID); err != nil {
				return err
			}
			moved = append(moved, c)
		}
		return nil
	})
	if txErr != nil {
		return txErr
	}

	for _, c := range moved {
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
	candidates, err := s.resolveCandidates(ctx, in.TenantID, active)
	if err != nil {
		return err
	}

	plans := newCompiledPlanCache(s.Definitions, s.logger())
	if in.Scope == "department" && in.ScopeID != nil {
		if err := s.requirePlans(ctx, plans, in.TenantID, candidates); err != nil {
			return err
		}
	}
	scoped := make([]delegationCandidate, 0, len(candidates))
	for _, c := range candidates {
		if scopeMatches(ctx, s.logger(), plans, in.TenantID, c, in.Scope, in.ScopeID) {
			scoped = append(scoped, c)
		}
	}
	if len(scoped) == 0 {
		return nil
	}

	eligible, err := s.eligibleCandidates(ctx, plans, in.TenantID, in.DelegateID, scoped, "delegation reroute")
	if err != nil {
		return err
	}
	if len(eligible) == 0 {
		return nil
	}

	commit := func(ctx context.Context) error {
		return s.commitReassignments(ctx, in.TenantID, eligible, in.DelegateID, delegationReasonTag(in.DelegationID), &in.DelegationID)
	}
	settled := instancesFullyMoved(candidates, eligible)
	if len(settled) == 0 {
		return commit(ctx)
	}
	return s.Locks.WithLock(ctx, port.UserAvailabilityScope(in.TenantID, in.DelegatorID), func(ctx context.Context) error {
		if err := s.resumeForAbsentDelegator(ctx, in.TenantID, in.DelegatorID, settled); err != nil {
			return err
		}
		return commit(ctx)
	})
}

// instancesFullyMoved returns one row per instance whose every task the
// delegator holds is being moved, so nothing the delegator still owns keeps the
// instance waiting for their absence. It does not look at the instance status:
// the row was read before the lock, and a pause can land after it.
func instancesFullyMoved(all, moved []delegationCandidate) []delegationCandidate {
	movedIDs := make(map[uuid.UUID]struct{}, len(moved))
	for _, c := range moved {
		movedIDs[c.assignment.ID] = struct{}{}
	}
	kept := make(map[uuid.UUID]struct{})
	for _, c := range all {
		if _, ok := movedIDs[c.assignment.ID]; !ok {
			kept[c.instance.ID] = struct{}{}
		}
	}
	var out []delegationCandidate
	seen := make(map[uuid.UUID]struct{})
	for _, c := range moved {
		if _, stays := kept[c.instance.ID]; stays {
			continue
		}
		if _, dup := seen[c.instance.ID]; dup {
			continue
		}
		seen[c.instance.ID] = struct{}{}
		out = append(out, c)
	}
	return out
}

// resumeForAbsentDelegator resumes the instances an out-of-office pause left
// waiting for a delegator whose work now has an available owner. The workflow
// only honours the resume when the pause came from out of office.
func (s *DelegationReconciler) resumeForAbsentDelegator(ctx context.Context, tenantID, delegatorID uuid.UUID, rows []delegationCandidate) error {
	absent, err := s.isOutOfOffice(ctx, tenantID, delegatorID)
	if err != nil || !absent {
		return err
	}
	return s.signalOncePerInstance(ctx, rows, port.SignalInstanceResume)
}

func (s *DelegationReconciler) Reverse(ctx context.Context, in port.DelegationReversalInput) error {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateID)
	if err != nil {
		return err
	}
	candidates, err := s.resolveCandidates(ctx, in.TenantID, active)
	if err != nil {
		return err
	}

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

	eligible, err := s.eligibleCandidates(ctx, newCompiledPlanCache(s.Definitions, s.logger()), in.TenantID, in.DelegatorID, tagged, "delegation reversal")
	if err != nil {
		return err
	}
	if len(eligible) == 0 {
		return nil
	}

	return s.Locks.WithLock(ctx, port.UserAvailabilityScope(in.TenantID, in.DelegatorID), func(ctx context.Context) error {
		if err := s.settleOutOfOfficePauses(ctx, in, eligible); err != nil {
			return err
		}
		return s.commitReassignments(ctx, in.TenantID, eligible, in.DelegatorID, "", &in.DelegationID)
	})
}

func (s *DelegationReconciler) settleOutOfOfficePauses(ctx context.Context, in port.DelegationReversalInput, rows []delegationCandidate) error {
	absent, err := s.isOutOfOffice(ctx, in.TenantID, in.DelegatorID)
	if err != nil {
		return err
	}
	switch {
	case !absent:
		return s.signalRestoredInstances(ctx, rows, port.SignalInstanceResume)
	case in.EndedReason == port.DelegationEndedReassigned:
		return nil
	default:
		return s.signalRestoredInstances(ctx, rows, port.SignalInstancePause)
	}
}

func (s *DelegationReconciler) isOutOfOffice(ctx context.Context, tenantID, userID uuid.UUID) (bool, error) {
	status, found, err := s.Availability.Get(ctx, tenantID, userID)
	if err != nil {
		return false, fmt.Errorf("delegation: read availability: %w", err)
	}
	return found && status == port.AvailabilityOOO, nil
}

func (s *DelegationReconciler) signalRestoredInstances(ctx context.Context, rows []delegationCandidate, signalName string) error {
	if signalName != port.SignalInstanceResume {
		return s.signalOncePerInstance(ctx, rows, signalName)
	}
	paused := make([]delegationCandidate, 0, len(rows))
	for _, c := range rows {
		if c.instance.Status == domain.InstanceStatusPaused {
			paused = append(paused, c)
		}
	}
	return s.signalOncePerInstance(ctx, paused, signalName)
}

func (s *DelegationReconciler) signalOncePerInstance(ctx context.Context, rows []delegationCandidate, signalName string) error {
	signalled := make(map[uuid.UUID]struct{}, len(rows))
	for _, c := range rows {
		if _, ok := signalled[c.instance.ID]; ok {
			continue
		}
		signalled[c.instance.ID] = struct{}{}
		wire := adminSignalWire{Initiator: domain.InitiatorOOO, RecordVersion: c.instance.RecordVersion}
		if err := s.Temporal.SignalWorkflow(ctx, c.instance.TemporalWorkflowID, c.instance.ID, signalName, wire); err != nil {
			return fmt.Errorf("delegation: %s instance %s: %w", signalName, c.instance.ID, err)
		}
	}
	return nil
}
