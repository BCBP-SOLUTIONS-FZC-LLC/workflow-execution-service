package service

import (
	"context"
	"fmt"
	"time"

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
	Signals      port.WorkflowSignalOutbox
	Definitions  port.DefinitionServiceClient
	Eligibility  port.EligibilityChecker
	Validator    port.EventValidator
	Availability port.UserAvailabilityRepository
	Locks        port.ScopeSetLocker
	Ended        port.EndedDelegationRepository
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
		return nil, fmt.Errorf("%w: %s: %d answers for %d requests", port.ErrEligibilityAnswerCount, logPrefix, len(answers), len(requests))
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

// commitReassignments moves the rows in one transaction and records, with each
// move, its event and its signal; the outbox relay delivers the signal after
// the commit. A row whose assignment ended after it was read gets no new
// assignment, no event and no signal. beforeSignal, when set, runs inside the same
// transaction for each row that moved, just before that row's own signal.
func (s *DelegationReconciler) commitReassignments(ctx context.Context, tenantID uuid.UUID, rows []delegationCandidate, newUserID uuid.UUID, reasonFor func(*domain.TaskAssignment) string, delegationID *uuid.UUID, leadsUnclaimed bool, beforeSignal func(context.Context, delegationCandidate) error) error {
	return s.Transactor.RunInTx(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		for _, c := range rows {
			active, ok, err := vacateIfActive(ctx, s.Assignments, tenantID, c.assignment)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if err := handOver(ctx, s.Assignments, s.Tasks, tenantID, c.assignment, active, c.task, newUserID, reasonFor(c.assignment)); err != nil {
				return err
			}
			if leadsUnclaimed {
				if err := leadUnclaimedHolder(ctx, s.Assignments, s.Tasks, tenantID, c.assignment, active, c.task.ID, newUserID); err != nil {
					return err
				}
			}
			if err := enqueueTaskReassigned(ctx, s.Outbox, s.Validator, c.instance, c.task, c.assignment.UserID, newUserID, delegationID); err != nil {
				return err
			}
			if beforeSignal != nil {
				if err := beforeSignal(ctx, c); err != nil {
					return err
				}
			}
			if err := recordWorkflowSignal(ctx, s.Signals, c.instance, port.SignalInstanceReassign, reassignSignalWire{
				TaskID: c.task.ID.String(), OldUserID: c.assignment.UserID.String(), NewUserID: newUserID.String(), RecordVersion: c.task.RecordVersion, RecordedAt: time.Now().UTC(),
			}); err != nil {
				return fmt.Errorf("delegation reconciler: record the signal of task %s: %w", c.task.ID, err)
			}
		}
		return nil
	})
}

func (s *DelegationReconciler) Reroute(ctx context.Context, in port.DelegationRerouteInput) error {
	if in.DelegateID == in.DelegatorID || (in.EndsAt != nil && !in.EndsAt.After(time.Now())) {
		return nil
	}
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

	return s.Locks.WithLocks(ctx, startScopes(in), func(ctx context.Context) error {
		stopped, err := s.endedBeforeMove(ctx, in)
		if err != nil || stopped {
			return err
		}
		return s.moveToDelegate(ctx, in, eligible)
	})
}

// startScopes are the locks a start holds while it moves work: the delegator's,
// which the end of the same delegation takes before it is recorded, and the
// delegation's and the delegate's, which a cancel by the delegate takes (the
// delegation's when it names one, the delegate's when it names none). A cancel
// therefore reads the delegate's rows only after a start that read "not ended"
// has committed its move, or finds the cancel recorded before it moves anything.
func startScopes(in port.DelegationRerouteInput) []string {
	return []string{
		port.UserAvailabilityScope(in.TenantID, in.DelegatorID),
		port.DelegationScope(in.TenantID, in.DelegationID),
		port.DelegateScope(in.TenantID, in.DelegateID),
	}
}

// endedBeforeMove reports whether the delegation has ended, or every delegation
// to the delegate was cancelled after this start event was emitted (execution
// LLD §6.2). The delegation's own start date is not compared: it is a business
// date from the producer and says nothing about when the delegation was created.
func (s *DelegationReconciler) endedBeforeMove(ctx context.Context, in port.DelegationRerouteInput) (bool, error) {
	ended, err := s.Ended.IsEnded(ctx, in.TenantID, in.DelegationID)
	if err != nil || ended {
		return ended, err
	}
	cancelledAt, found, err := s.Ended.DelegateEndedAt(ctx, in.TenantID, in.DelegateID)
	if err != nil {
		return false, err
	}
	return found && !in.EmittedAt.IsZero() && cancelledAt.After(in.EmittedAt), nil
}

// moveToDelegate runs under the locks of startScopes. The delegator's rows are
// listed again here, because the list read before the lock may have lost a row
// to a concurrent change or missed one it gained; only rows still held count as
// moved, and an instance settles only when no other row of the delegator's is
// left on it.
func (s *DelegationReconciler) moveToDelegate(ctx context.Context, in port.DelegationRerouteInput, eligible []delegationCandidate) error {
	held, err := s.delegatorRows(ctx, in)
	if err != nil {
		return err
	}
	var recordResume func(context.Context, delegationCandidate) error
	if settled := instancesFullyMoved(held, stillHeld(eligible, held)); len(settled) > 0 {
		absent, err := s.isOutOfOffice(ctx, in.TenantID, in.DelegatorID)
		if err != nil {
			return err
		}
		if absent {
			recordResume = s.instanceSignalRecorder(settled, port.SignalInstanceResume)
		}
	}
	hop := delegationHop(in.DelegationID, in.DelegatorID)
	return s.commitReassignments(ctx, in.TenantID, eligible, in.DelegateID, func(a *domain.TaskAssignment) string {
		return appendDelegationHop(a.Reason, hop)
	}, &in.DelegationID, true, recordResume)
}

func (s *DelegationReconciler) delegatorRows(ctx context.Context, in port.DelegationRerouteInput) ([]delegationCandidate, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegatorID)
	if err != nil {
		return nil, err
	}
	return s.resolveCandidates(ctx, in.TenantID, active)
}

// stillHeld keeps the rows of moving that the delegator still holds.
func stillHeld(moving, held []delegationCandidate) []delegationCandidate {
	heldIDs := make(map[uuid.UUID]struct{}, len(held))
	for _, c := range held {
		heldIDs[c.assignment.ID] = struct{}{}
	}
	out := make([]delegationCandidate, 0, len(moving))
	for _, c := range moving {
		if _, ok := heldIDs[c.assignment.ID]; ok {
			out = append(out, c)
		}
	}
	return out
}

// instancesFullyMoved returns one row per instance whose every task the
// delegator holds is being moved, so nothing the delegator still owns keeps the
// instance waiting for their absence. It does not look at the instance status:
// a pause can land after the row was read.
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

// instanceSignalRecorder returns the step that records, inside the move's
// transaction, one pause or resume signal for an instance among the rows, the
// first time a row of that instance has actually moved; an instance whose rows
// all lost the race gets none. The signal is tagged as out of office: the
// workflow only honours a resume when the pause came from out of office.
func (s *DelegationReconciler) instanceSignalRecorder(rows []delegationCandidate, signalName string) func(context.Context, delegationCandidate) error {
	wanted := make(map[uuid.UUID]struct{}, len(rows))
	for _, c := range rows {
		wanted[c.instance.ID] = struct{}{}
	}
	return func(ctx context.Context, moved delegationCandidate) error {
		id := moved.instance.ID
		if _, ok := wanted[id]; !ok {
			return nil
		}
		wire := adminSignalWire{Initiator: domain.InitiatorOOO, RecordVersion: moved.instance.RecordVersion}
		if err := recordWorkflowSignal(ctx, s.Signals, moved.instance, signalName, wire); err != nil {
			return fmt.Errorf("delegation: record %s of instance %s: %w", signalName, id, err)
		}
		delete(wanted, id)
		return nil
	}
}

func (s *DelegationReconciler) Reverse(ctx context.Context, in port.DelegationReversalInput) error {
	if in.DelegatorID == in.DelegateID {
		return nil
	}
	if err := s.recordEnded(ctx, in); err != nil {
		return err
	}
	tag := delegationReasonTag(in.DelegationID)
	carried, err := s.Assignments.ListActiveByReasonTag(ctx, in.TenantID, tag)
	if err != nil {
		return err
	}
	owed := make([]*domain.TaskAssignment, 0, len(carried))
	for _, a := range carried {
		if reasonHasDelegation(a.Reason, tag) && a.UserID != in.DelegatorID {
			owed = append(owed, a)
		}
	}
	tagged, err := s.resolveCandidates(ctx, in.TenantID, owed)
	if err != nil {
		return err
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
		recordSignals, err := s.outOfOfficePauseRecorder(ctx, in, eligible)
		if err != nil {
			return err
		}
		return s.commitReassignments(ctx, in.TenantID, eligible, in.DelegatorID, func(a *domain.TaskAssignment) string {
			return reasonBeforeDelegation(a.Reason, tag)
		}, &in.DelegationID, false, recordSignals)
	})
}

func (s *DelegationReconciler) recordEnded(ctx context.Context, in port.DelegationReversalInput) error {
	return s.Locks.WithLock(ctx, port.UserAvailabilityScope(in.TenantID, in.DelegatorID), func(ctx context.Context) error {
		return s.Ended.MarkEnded(ctx, in.TenantID, in.DelegationID)
	})
}

// outOfOfficePauseRecorder reads the delegator's availability, under the caller's
// lock, and returns the step that records the pause or resume the restored work
// needs; it is nil when none is needed. The move runs it inside its transaction
// for each row that moved.
func (s *DelegationReconciler) outOfOfficePauseRecorder(ctx context.Context, in port.DelegationReversalInput, rows []delegationCandidate) (func(context.Context, delegationCandidate) error, error) {
	absent, err := s.isOutOfOffice(ctx, in.TenantID, in.DelegatorID)
	if err != nil {
		return nil, err
	}
	var signalName string
	switch {
	case !absent:
		signalName = port.SignalInstanceResume
	case in.EndedReason == port.DelegationEndedReassigned:
		return nil, nil
	default:
		signalName = port.SignalInstancePause
	}
	return s.instanceSignalRecorder(rows, signalName), nil
}

func (s *DelegationReconciler) isOutOfOffice(ctx context.Context, tenantID, userID uuid.UUID) (bool, error) {
	status, found, err := s.Availability.Get(ctx, tenantID, userID)
	if err != nil {
		return false, fmt.Errorf("delegation: read availability: %w", err)
	}
	return found && status == port.AvailabilityOOO, nil
}
