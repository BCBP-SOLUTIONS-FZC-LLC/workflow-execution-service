package service

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.WorkflowClient = (*WorkflowClient)(nil)

type WorkflowClient struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Transactor  port.Transactor
	Outbox      port.OutboxRepository
	Signals     port.WorkflowSignalOutbox
	Validator   port.EventValidator
	Eligibility port.EligibilityChecker
	Definitions port.DefinitionServiceClient
	Ended       port.EndedDelegationRepository
	Locks       port.ScopeSetLocker
	Log         port.Logger
}

func (s *WorkflowClient) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func delegationReasonTag(delegationID uuid.UUID) string {
	return delegationTagPrefix + delegationID.String()
}

// delegatedAssignments selects the assignments a delegation created: those that
// carry the delegation's tag, or any delegation's tag when none is named.
func delegatedAssignments(assignments []*domain.TaskAssignment, delegationID *uuid.UUID) []*domain.TaskAssignment {
	var out []*domain.TaskAssignment
	for _, a := range assignments {
		if delegationID == nil && isDelegated(a.Reason) || delegationID != nil && reasonHasDelegation(a.Reason, delegationReasonTag(*delegationID)) {
			out = append(out, a)
		}
	}
	return out
}

type delegateRow struct {
	assignment *domain.TaskAssignment
	task       *domain.Task
	instance   *domain.Instance
	restore    *delegationRestore
}

// delegationRestore is the user a cancelled row goes back to, with the reason the
// row carries there.
type delegationRestore struct {
	userID       uuid.UUID
	delegationID uuid.UUID
	reason       string
}

// ReassignDelegate reads everything it needs first, so a failed read changes
// nothing; it then moves the rows and records each one's event and signal in one
// transaction, and the outbox relay delivers the signals after the commit
// (execution LLD §5.8).
func (s *WorkflowClient) ReassignDelegate(ctx context.Context, in port.ReassignDelegateInput) (int, error) {
	if in.NewDelegateID == in.OldDelegateID {
		return 0, nil
	}
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.OldDelegateID)
	if err != nil {
		return 0, err
	}
	rows, err := s.eligibleDelegateRows(ctx, in, delegatedAssignments(active, in.DelegationID))
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	moved, err := s.moveToNewDelegate(ctx, in, rows)
	if err != nil {
		return 0, err
	}
	return moved, nil
}

func (s *WorkflowClient) eligibleDelegateRows(ctx context.Context, in port.ReassignDelegateInput, matched []*domain.TaskAssignment) ([]delegateRow, error) {
	plans := newCompiledPlanCache(s.Definitions, s.logger())
	answered := map[eligibilityPair]bool{}
	rows := make([]delegateRow, 0, len(matched))
	for _, a := range matched {
		task, err := s.Tasks.GetByID(ctx, in.TenantID, a.TaskID)
		if err != nil {
			return nil, fmt.Errorf("reassign delegate: read task of assignment %s: %w", a.ID, err)
		}
		inst, err := s.Instances.GetByID(ctx, in.TenantID, task.WorkflowInstanceID)
		if err != nil {
			return nil, fmt.Errorf("reassign delegate: read instance of task %s: %w", task.ID, err)
		}
		eligible, err := s.checkAssigneeEligible(ctx, plans, answered, in.TenantID, task, inst, in.NewDelegateID)
		if err != nil {
			return nil, err
		}
		if !eligible {
			s.logger().Warn("skipping assignment: new delegate ineligible for task", map[string]any{"task_id": task.ID})
			continue
		}
		rows = append(rows, delegateRow{assignment: a, task: task, instance: inst})
	}
	return rows, nil
}

// moveToNewDelegate returns the number of rows it moved, each with its event and
// its signal: a row whose assignment ended after it was read is left alone, and
// gets neither.
func (s *WorkflowClient) moveToNewDelegate(ctx context.Context, in port.ReassignDelegateInput, rows []delegateRow) (int, error) {
	moved := 0
	err := s.Transactor.RunInTx(withTenantGUC(ctx, in.TenantID), func(ctx context.Context) error {
		moved = 0
		for _, r := range rows {
			active, ok, err := vacateIfActive(ctx, s.Assignments, in.TenantID, r.assignment)
			if err != nil {
				return fmt.Errorf("reassign delegate: vacate assignment %s: %w", r.assignment.ID, err)
			}
			if !ok {
				continue
			}
			if err := handOver(ctx, s.Assignments, s.Tasks, in.TenantID, r.assignment, active, r.task, in.NewDelegateID, r.assignment.Reason); err != nil {
				return fmt.Errorf("reassign delegate: hand task %s to the new delegate: %w", r.task.ID, err)
			}
			if err := enqueueTaskReassigned(ctx, s.Outbox, s.Validator, r.instance, r.task, r.assignment.UserID, in.NewDelegateID, in.DelegationID); err != nil {
				return fmt.Errorf("reassign delegate: record the reassigned event of task %s: %w", r.task.ID, err)
			}
			if err := s.recordReassign(ctx, r, in.NewDelegateID); err != nil {
				return fmt.Errorf("reassign delegate: record the signal of task %s: %w", r.task.ID, err)
			}
			moved++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return moved, nil
}

// CancelByDelegate runs under the lock a start of the same delegation takes: the
// named delegation's, or the delegate's when none is named, so a start either
// commits its move before the cancel reads the delegate's rows or finds the end
// recorded (execution LLD §6.2).
func (s *WorkflowClient) CancelByDelegate(ctx context.Context, in port.CancelByDelegateInput) (int, error) {
	scope := port.DelegateScope(in.TenantID, in.DelegateUserID)
	if in.DelegationID != nil {
		scope = port.DelegationScope(in.TenantID, *in.DelegationID)
	}
	var cancelled int
	err := s.Locks.WithLock(ctx, scope, func(ctx context.Context) (err error) {
		cancelled, err = s.cancelByDelegate(ctx, in)
		return err
	})
	if err != nil {
		return 0, err
	}
	return cancelled, nil
}

// cancelByDelegate reads the task and instance of every selected assignment
// first, then vacates them, and records the cancel as ended, in one
// transaction; a task left with no active
// assignee gets a signal recorded in the same transaction, so the workflow
// learns of it (execution LLD §5.8). A failed read or write changes nothing
// and fails the call so the caller retries.
func (s *WorkflowClient) cancelByDelegate(ctx context.Context, in port.CancelByDelegateInput) (int, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateUserID)
	if err != nil {
		return 0, err
	}
	matched := delegatedAssignments(active, in.DelegationID)
	if len(matched) == 0 {
		return 0, s.markDelegationEnded(ctx, in)
	}
	rows, err := s.readDelegateRows(ctx, in.TenantID, matched)
	if err != nil {
		return 0, err
	}
	if err := s.planRestores(ctx, in, rows); err != nil {
		return 0, err
	}
	vacated := 0
	err = s.Transactor.RunInTx(withTenantGUC(ctx, in.TenantID), func(ctx context.Context) error {
		vacated = 0
		if err := s.markDelegationEnded(ctx, in); err != nil {
			return err
		}
		for _, r := range rows {
			remaining, ok, err := vacateIfActive(ctx, s.Assignments, in.TenantID, r.assignment)
			if err != nil {
				return fmt.Errorf("cancel by delegate: vacate assignment %s: %w", r.assignment.ID, err)
			}
			if !ok {
				continue
			}
			vacated++
			if r.restore != nil {
				if err := s.returnToDelegator(ctx, in.TenantID, r, remaining); err != nil {
					return err
				}
				continue
			}
			if len(remaining) > 1 || isTerminalInstanceStatus(r.instance.Status) {
				continue
			}
			if err := s.recordTaskVacated(ctx, r); err != nil {
				return fmt.Errorf("cancel by delegate: record the signal of task %s: %w", r.task.ID, err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return vacated, nil
}

// markDelegationEnded records the named delegation as ended, so a start of it
// delivered later moves nothing; a cancel that names none records the time for
// the delegate, so a start of any delegation that began before it moves nothing.
func (s *WorkflowClient) markDelegationEnded(ctx context.Context, in port.CancelByDelegateInput) error {
	if in.DelegationID == nil {
		if err := s.Ended.MarkDelegateEnded(ctx, in.TenantID, in.DelegateUserID, time.Now().UTC()); err != nil {
			return fmt.Errorf("cancel by delegate: record every delegation to %s as ended: %w", in.DelegateUserID, err)
		}
		return nil
	}
	if err := s.Ended.MarkEnded(ctx, in.TenantID, *in.DelegationID); err != nil {
		return fmt.Errorf("cancel by delegate: record delegation %s as ended: %w", *in.DelegationID, err)
	}
	return nil
}

// planRestores marks each row whose delegation recorded who the work was taken
// from, when that user is eligible for the task again. The others are only vacated.
func (s *WorkflowClient) planRestores(ctx context.Context, in port.CancelByDelegateInput, rows []delegateRow) error {
	plans := newCompiledPlanCache(s.Definitions, s.logger())
	answered := map[uuid.UUID]map[eligibilityPair]bool{}
	for i := range rows {
		r := &rows[i]
		hop, ok := hopToUnwind(r.assignment.Reason, in.DelegationID)
		if !ok {
			continue
		}
		delegationID, delegatorID, ok := parseDelegationHop(hop)
		if !ok || delegatorID == r.assignment.UserID {
			continue
		}
		if answered[delegatorID] == nil {
			answered[delegatorID] = map[eligibilityPair]bool{}
		}
		eligible, err := s.checkAssigneeEligible(ctx, plans, answered[delegatorID], in.TenantID, r.task, r.instance, delegatorID)
		if err != nil {
			return err
		}
		if eligible {
			r.restore = &delegationRestore{userID: delegatorID, delegationID: delegationID, reason: reasonBeforeDelegation(r.assignment.Reason, hop)}
		}
	}
	return nil
}

func (s *WorkflowClient) returnToDelegator(ctx context.Context, tenantID uuid.UUID, r delegateRow, active []*domain.TaskAssignment) error {
	restore := r.restore
	if err := handOver(ctx, s.Assignments, s.Tasks, tenantID, r.assignment, active, r.task, restore.userID, restore.reason); err != nil {
		return fmt.Errorf("cancel by delegate: return task %s to its delegator: %w", r.task.ID, err)
	}
	if err := enqueueTaskReassigned(ctx, s.Outbox, s.Validator, r.instance, r.task, r.assignment.UserID, restore.userID, &restore.delegationID); err != nil {
		return fmt.Errorf("cancel by delegate: record the reassigned event of task %s: %w", r.task.ID, err)
	}
	if err := s.recordReassign(ctx, r, restore.userID); err != nil {
		return fmt.Errorf("cancel by delegate: record the signal of task %s: %w", r.task.ID, err)
	}
	return nil
}

func (s *WorkflowClient) readDelegateRows(ctx context.Context, tenantID uuid.UUID, assignments []*domain.TaskAssignment) ([]delegateRow, error) {
	rows := make([]delegateRow, 0, len(assignments))
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			return nil, fmt.Errorf("cancel by delegate: read task of assignment %s: %w", a.ID, err)
		}
		inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
		if err != nil {
			return nil, fmt.Errorf("cancel by delegate: read instance of task %s: %w", task.ID, err)
		}
		if isClosedTaskStatus(task.Status) {
			continue
		}
		rows = append(rows, delegateRow{assignment: a, task: task, instance: inst})
	}
	return rows, nil
}

func isClosedTaskStatus(status domain.TaskStatus) bool {
	return status == domain.TaskStatusCompleted || status == domain.TaskStatusFailed ||
		status == domain.TaskStatusSuperseded
}

func (s *WorkflowClient) DelegateImpact(ctx context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateUserID)
	if err != nil {
		return port.DelegateImpactResult{}, err
	}
	matched := delegatedAssignments(active, in.DelegationID)
	affected, err := s.affectedWorkflows(ctx, in.TenantID, matched)
	if err != nil {
		return port.DelegateImpactResult{}, err
	}
	ids, next := pageAffectedWorkflows(affected, in.Page)

	return port.DelegateImpactResult{
		ActiveWorkflows:   len(affected),
		ActiveAssignments: len(matched),
		WorkflowIDs:       port.PageResult[uuid.UUID]{Items: ids, NextCursor: next},
	}, nil
}

type affectedWorkflow struct {
	id        uuid.UUID
	createdAt time.Time
}

func (s *WorkflowClient) affectedWorkflows(ctx context.Context, tenantID uuid.UUID, assignments []*domain.TaskAssignment) ([]affectedWorkflow, error) {
	seen := make(map[uuid.UUID]struct{})
	var affected []affectedWorkflow
	for _, a := range assignments {
		task, err := s.Tasks.GetByID(ctx, tenantID, a.TaskID)
		if err != nil {
			return nil, fmt.Errorf("delegate impact: read task of assignment %s: %w", a.ID, err)
		}
		if _, ok := seen[task.WorkflowInstanceID]; ok {
			continue
		}
		seen[task.WorkflowInstanceID] = struct{}{}
		affected = append(affected, affectedWorkflow{id: task.WorkflowInstanceID, createdAt: task.CreatedAt})
	}
	sort.Slice(affected, func(i, j int) bool {
		return bytes.Compare(affected[i].id[:], affected[j].id[:]) < 0
	})
	return affected, nil
}

func pageAffectedWorkflows(affected []affectedWorkflow, page port.Page) ([]uuid.UUID, string) {
	remaining := affected
	if page.Cursor != nil {
		after := page.Cursor.ID
		remaining = remaining[sort.Search(len(remaining), func(i int) bool {
			return bytes.Compare(remaining[i].id[:], after[:]) > 0
		}):]
	}
	limit := page.Limit
	if limit <= 0 || limit > len(remaining) {
		limit = len(remaining)
	}
	ids := make([]uuid.UUID, limit)
	for i, w := range remaining[:limit] {
		ids[i] = w.id
	}
	if limit == len(remaining) {
		return ids, ""
	}
	last := remaining[limit-1]
	return ids, port.EncodeCursor(port.CursorPosition{CreatedAt: last.createdAt, ID: last.id})
}

func (s *WorkflowClient) checkAssigneeEligible(ctx context.Context, plans *compiledPlanCache, answered map[eligibilityPair]bool, tenantID uuid.UUID, task *domain.Task, inst *domain.Instance, newUserID uuid.UUID) (bool, error) {
	if s.Eligibility == nil || s.Definitions == nil {
		return false, errEligibilityNotWired
	}
	plan, err := plans.mainPlan(ctx, tenantID, inst.WorkflowVersionID)
	if err != nil {
		return false, fmt.Errorf("reassign delegate: read compiled plan of task %s: %w", task.ID, err)
	}
	level, ok := requiredLevelForTask(plan, task)
	if !ok {
		return false, nil
	}
	pair := eligibilityPair{department: task.DepartmentID, level: level}
	if eligible, seen := answered[pair]; seen {
		return eligible, nil
	}
	eligible, err := s.Eligibility.CheckEligibility(ctx, tenantID, newUserID, task.DepartmentID, level)
	if err != nil {
		return false, fmt.Errorf("%w: %w", port.ErrEligibilityRead, err)
	}
	answered[pair] = eligible
	return eligible, nil
}

func (s *WorkflowClient) recordReassign(ctx context.Context, r delegateRow, newUserID uuid.UUID) error {
	return recordWorkflowSignal(ctx, s.Signals, r.instance, port.SignalInstanceReassign, reassignSignalWire{
		TaskID: r.task.ID.String(), OldUserID: r.assignment.UserID.String(), NewUserID: newUserID.String(),
		RecordVersion: r.task.RecordVersion, RecordedAt: time.Now().UTC(),
	})
}

func (s *WorkflowClient) recordTaskVacated(ctx context.Context, r delegateRow) error {
	return recordWorkflowSignal(ctx, s.Signals, r.instance, port.SignalInstanceTaskVacated, taskVacatedSignalWire{
		TaskID: r.task.ID.String(), VacatedUserID: r.assignment.UserID.String(), RecordedAt: time.Now().UTC(),
	})
}
