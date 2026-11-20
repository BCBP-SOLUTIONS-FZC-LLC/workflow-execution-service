package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

var _ port.TaskService = (*TaskService)(nil)

// errEligibilityNotWired refuses an action from a service built without the collaborators the
// eligibility check needs, rather than letting the user through unchecked.
var errEligibilityNotWired = errors.New("service has no eligibility checker or definitions client: refusing to act without the eligibility check")

type TaskService struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Overrides   port.AssigneeOverrideRepository
	Transactor  port.Transactor
	Outbox      port.OutboxRepository
	Signals     port.WorkflowSignalOutbox
	Validator   port.EventValidator
	Temporal    port.TemporalClient
	Eligibility port.EligibilityChecker
	Definitions port.DefinitionServiceClient
	Cache       port.CacheStore
	Log         port.Logger
}

func (s *TaskService) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

// taskVacatedSignalWire is the payload of instance-task-vacated. RecordedAt lets
// the workflow order it against a reassignment, because the relay does not.
type taskVacatedSignalWire struct {
	TaskID        string
	VacatedUserID string
	RecordedAt    time.Time
}

// reassignSignalWire is the payload of instance-reassign. RecordedAt is set by
// the delegation paths, whose signals go through the outbox; a direct admin
// reassignment leaves it out.
type reassignSignalWire struct {
	TaskID        string
	OldUserID     string `json:",omitempty"`
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
	RecordedAt    time.Time `json:",omitzero"`
}

func deptAndSuffix(nodeKey string) (deptID, suffix string) {
	deptID, suffix, _ = strings.Cut(nodeKey, "/")
	return deptID, suffix
}

func (s *TaskService) List(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
	repoFilter := port.TaskListFilter{
		WorkflowInstanceID: filter.WorkflowInstanceID,
		DepartmentID:       filter.DepartmentID,
		AssigneeUserID:     filter.AssigneeUserID,
		DueBefore:          filter.DueBefore,
	}
	if filter.Status != nil {
		status := domain.TaskStatus(*filter.Status)
		repoFilter.Status = &status
	}
	if !scope.IsAdmin {
		repoFilter.Scope = readScopeFilter(scope)
	}

	rows, next, err := s.Tasks.ListByTenant(ctx, tenantID, repoFilter, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.Task]{}, wrapTaskErr(err)
	}
	items, err := toPortTasksWithCounts(ctx, s.Assignments, tenantID, rows)
	if err != nil {
		return port.PageResult[*port.Task]{}, err
	}
	return port.PageResult[*port.Task]{Items: items, NextCursor: encodeNextCursor(next)}, nil
}

func (s *TaskService) Get(ctx context.Context, tenantID, taskID uuid.UUID, scope port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, nil, wrapTaskErr(err)
	}
	assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, taskID)
	if err != nil {
		return nil, nil, err
	}

	if !scope.IsAdmin && !s.taskInScope(scope, task, assignments) {
		return nil, nil, port.ErrNotAuthorizedForRead
	}

	portAssignments := make([]*port.TaskAssignment, len(assignments))
	for i, a := range assignments {
		portAssignments[i] = toPortTaskAssignment(a)
	}
	out := toPortTask(task)
	out.AssigneeCount = len(assignments)
	if !scope.IsAdmin && len(out.ExtrasJSON) > 0 {
		out.ExtrasJSON = withoutConnectorFields(out.ExtrasJSON)
	}
	return out, portAssignments, nil
}

func (s *TaskService) taskInScope(scope port.ReadScope, task *domain.Task, assignments []*domain.TaskAssignment) bool {
	for _, d := range scope.Departments {
		if d.DepartmentID == task.DepartmentID {
			return true
		}
	}
	for _, a := range assignments {
		if a.UserID == scope.CallerUserID {
			return true
		}
	}
	return false
}

func (s *TaskService) ActiveByUser(ctx context.Context, tenantID, userID uuid.UUID, page port.Page) (port.PageResult[*port.ActiveUserTask], error) {
	rows, next, err := s.Assignments.ListActiveByUserPaginated(ctx, tenantID, userID, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.ActiveUserTask]{}, err
	}
	items := make([]*port.ActiveUserTask, len(rows))
	for i, row := range rows {
		items[i] = toPortActiveUserTask(row)
	}
	return port.PageResult[*port.ActiveUserTask]{Items: items, NextCursor: encodeNextCursor(next)}, nil
}

func (s *TaskService) checkActorEligible(ctx context.Context, tenantID uuid.UUID, task *domain.Task, userID uuid.UUID) error {
	if s.Eligibility == nil || s.Definitions == nil {
		return errEligibilityNotWired
	}
	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	return s.checkEligible(ctx, inst, task, userID)
}

func checkHumanActionable(task *domain.Task) error {
	if task.ConnectorType != nil && *task.ConnectorType != "" {
		return port.ErrTaskNotHumanActionable
	}
	return nil
}

func (s *TaskService) currentAssignee(ctx context.Context, tenantID uuid.UUID, task *domain.Task, userID uuid.UUID) (*domain.TaskAssignment, error) {
	assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, task.ID)
	if err != nil {
		return nil, err
	}
	if task.AssigneeMode == "all" {
		for _, a := range assignments {
			if a.IsLead {
				if a.UserID != userID {
					return nil, port.ErrNotAssignee
				}
				return a, nil
			}
		}
		return nil, port.ErrNotAssignee
	}
	for _, a := range assignments {
		if a.UserID == userID {
			return a, nil
		}
	}
	return nil, port.ErrNotAssignee
}

func (s *TaskService) Claim(ctx context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	if err := checkHumanActionable(task); err != nil {
		return nil, err
	}
	if task.AssigneeMode != "all" {
		return nil, port.ErrClaimNotApplicable
	}
	if task.RecordVersion != recordVersion {
		return nil, staleTaskVersion()
	}
	if err := checkTaskActive(task); err != nil {
		return nil, err
	}
	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	if err := checkInstanceActionable(inst); err != nil {
		return nil, err
	}

	assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	var target *domain.TaskAssignment
	leadHeld := false
	for _, a := range assignments {
		if a.UserID == userID {
			target = a
		}
		leadHeld = leadHeld || a.IsLead
	}
	if target == nil {
		return nil, port.ErrNotAssignee
	}
	if target.IsLead {
		return s.presentedTask(ctx, tenantID, task), nil
	}
	if leadHeld {
		return nil, port.ErrTaskAlreadyClaimed
	}
	if err := s.checkActorEligible(ctx, tenantID, task, userID); err != nil {
		return nil, err
	}

	err = s.Transactor.RunInTx(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		if _, err := s.Assignments.ClaimLead(ctx, tenantID, taskID, target.ID, recordVersion); err != nil {
			return claimLeadErr(err)
		}
		return s.enqueueTaskClaimed(ctx, inst, task, assignments, userID)
	})
	if err != nil {
		return nil, err
	}
	committed := *task
	committed.RecordVersion++
	return s.readCommitted(ctx, tenantID, &committed)
}

func (s *TaskService) enqueueTaskClaimed(ctx context.Context, inst *domain.Instance, task *domain.Task, assignments []*domain.TaskAssignment, claimedBy uuid.UUID) error {
	assignees := make([]uuid.UUID, len(assignments))
	for i, a := range assignments {
		assignees[i] = a.UserID
	}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assignees}
	payload := domain.NewWorkflowTaskClaimedPayload(instanceCore(inst), taskCore, claimedBy)
	envelope, err := BuildEnvelope(ctx, s.Validator, domain.EventWorkflowTaskClaimed, inst.TenantID.String(), "instances/"+inst.ID.String(), claimedBy.String(), payload)
	if err != nil {
		return err
	}
	return s.Outbox.Enqueue(ctx, envelope)
}

func claimLeadErr(err error) error {
	if errors.Is(err, domain.ErrNotFound) {
		return port.ErrNotAssignee
	}
	return wrapTaskErr(err)
}

// readCommitted answers with the task as stored now. A read-back that fails twice does not undo a change
// that already committed, so the caller's own picture of the committed state is returned instead; a task
// that is gone is still reported as not found.
func (s *TaskService) readCommitted(ctx context.Context, tenantID uuid.UUID, committed *domain.Task) (*port.Task, error) {
	for range 2 {
		task, err := s.Tasks.GetByID(ctx, tenantID, committed.ID)
		if err == nil {
			return s.presentedTask(ctx, tenantID, task), nil
		}
		if errors.Is(err, domain.ErrNotFound) {
			return nil, wrapTaskErr(err)
		}
		s.logger().Warn("task read-back after a committed change failed", map[string]any{"task_id": committed.ID, "error": err.Error()})
	}
	return s.presentedTask(ctx, tenantID, committed), nil
}

// presentedTask maps a task an action just acted on and adds its active assignee count. The action has
// already taken effect, so a failed count answers without the count instead of failing the answer or
// reporting zero.
func (s *TaskService) presentedTask(ctx context.Context, tenantID uuid.UUID, task *domain.Task) *port.Task {
	counted, err := toPortTasksWithCounts(ctx, s.Assignments, tenantID, []*domain.Task{task})
	if err != nil {
		s.logger().Warn("assignee count after a task action failed", map[string]any{"task_id": task.ID, "error": err.Error()})
		out := toPortTask(task)
		out.AssigneeCountUnavailable = true
		return out
	}
	return counted[0]
}

type stageTransitionWire struct {
	DeptID        string
	ToStage       string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
	Failed        bool
	Reason        string
	VisitCount    int64
}

func (s *TaskService) Complete(ctx context.Context, tenantID, taskID, userID uuid.UUID, resultJSON json.RawMessage, recordVersion int64) (*port.Task, error) {
	task, err := s.precheckTaskAction(ctx, tenantID, taskID, userID, recordVersion)
	if err != nil {
		return nil, err
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	if inst.Status != domain.InstanceStatusRunning && inst.Status != domain.InstanceStatusDegraded {
		return nil, port.ErrInvalidInstanceState
	}
	if err := s.reserveCompletion(ctx, task.ID); err != nil {
		return nil, err
	}
	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-transition", stageTransitionWire{
		DeptID: deptID, NodeID: nodeID, UserID: userID.String(), ResultJSON: string(resultJSON), RecordVersion: recordVersion,
		VisitCount: task.VisitCount,
	}); err != nil {
		if !signalMayHaveBeenAccepted(err) {
			s.releaseCompletion(ctx, task.ID)
		}
		return nil, fmt.Errorf("signal stage-transition: %w", err)
	}
	s.markCompletionDelivered(ctx, task.ID)
	return s.presentedTask(ctx, tenantID, task), nil
}

type stageDeferWire struct {
	DeptID        string
	FromStage     string
	Reason        string
	UserID        string
	RecordVersion int64
	TaskID        string
	AssignmentID  string
}

func (s *TaskService) Defer(ctx context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*port.Task, error) {
	task, err := s.precheckTaskAction(ctx, tenantID, taskID, userID, recordVersion)
	if err != nil {
		return nil, err
	}
	assignment, err := s.currentAssignee(ctx, tenantID, task, userID)
	if err != nil {
		return nil, err
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	if inst.Status != domain.InstanceStatusRunning && inst.Status != domain.InstanceStatusDegraded {
		return nil, port.ErrInvalidInstanceState
	}
	if done, err := hasCompletedStep(ctx, s.Tasks, tenantID, inst.ID); err != nil {
		return nil, err
	} else if !done {
		return nil, port.ErrInvalidTaskState
	}
	deptID, nodeID := deptAndSuffix(task.NodeKey)
	err = s.Temporal.UpdateWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-defer", stageDeferWire{
		DeptID: deptID, FromStage: nodeID, Reason: reason, UserID: userID.String(), RecordVersion: recordVersion,
		TaskID: task.ID.String(), AssignmentID: assignment.ID.String(),
	})
	if errors.Is(err, port.ErrUpdateRefused) {
		return nil, fmt.Errorf("%w: %w", port.ErrInvalidTaskState, err)
	}
	if errors.Is(err, port.ErrRecordVersionConflict) {
		return nil, staleTaskVersion()
	}
	if err != nil {
		return nil, fmt.Errorf("update stage-defer: %w", err)
	}
	committed := *task
	committed.Status = domain.TaskStatusDeferred
	committed.RecordVersion += 2 // the status change and the completion of the deferring assignment each bump it
	return s.readCommitted(ctx, tenantID, &committed)
}

func (s *TaskService) Reassign(ctx context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, recordVersion int64) (*port.Task, error) {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	if err := checkHumanActionable(task); err != nil {
		return nil, err
	}
	if task.RecordVersion != recordVersion {
		return nil, staleTaskVersion()
	}
	if err := checkTaskActive(task); err != nil {
		return nil, err
	}

	assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	oldUserID := replacedUser(task, assignments)
	if oldUserID == newUserID {
		return nil, port.ErrOverrideNoOp
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	if err := checkInstanceActionable(inst); err != nil {
		return nil, err
	}
	if err := s.sendReassignSignal(ctx, inst, task, oldUserID, newUserID, actorUserID, recordVersion); err != nil {
		return nil, err
	}
	return s.presentedTask(ctx, tenantID, task), nil
}

func checkInstanceActionable(inst *domain.Instance) error {
	if inst.Status != domain.InstanceStatusRunning && inst.Status != domain.InstanceStatusDegraded {
		return port.ErrInvalidInstanceState
	}
	return nil
}

// replacedUser is the assignee a reassignment takes the task from: the only assignee, or the lead of an
// all-assignee task. It is the zero id when nobody holds the task.
func replacedUser(task *domain.Task, assignments []*domain.TaskAssignment) uuid.UUID {
	for _, a := range assignments {
		if task.AssigneeMode != "all" || a.IsLead {
			return a.UserID
		}
	}
	return uuid.Nil
}

func previousUserWire(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func staleTaskVersion() error {
	observability.IncCounterVec(observability.RecordVersionConflictTotal, "workflow_task")
	return port.ErrRecordVersionConflict
}

func (s *TaskService) sendReassignSignal(ctx context.Context, inst *domain.Instance, task *domain.Task, oldUserID, newUserID, actorUserID uuid.UUID, recordVersion int64) error {
	if err := s.checkEligible(ctx, inst, task, newUserID); err != nil {
		return err
	}
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceReassign, reassignSignalWire{
		TaskID: task.ID.String(), OldUserID: previousUserWire(oldUserID), NewUserID: newUserID.String(),
		AdminUserID: actorUserID.String(), RecordVersion: recordVersion,
	}); err != nil {
		return fmt.Errorf("signal instance-reassign: %w", err)
	}
	return nil
}

func (s *TaskService) checkEligible(ctx context.Context, inst *domain.Instance, task *domain.Task, userID uuid.UUID) error {
	if s.Eligibility == nil || s.Definitions == nil {
		return errEligibilityNotWired
	}
	plan, err := newCompiledPlanCache(s.Definitions, s.logger()).backedBy(s.Cache).mainPlan(ctx, inst.TenantID, inst.WorkflowVersionID)
	if err != nil {
		return fmt.Errorf("get compiled plan for eligibility check: %w", err)
	}
	level, ok := requiredLevelForTask(plan, task)
	if !ok {
		return fmt.Errorf("task %s has no stage in its compiled plan: %w", task.ID, port.ErrAssigneeIneligible)
	}
	eligible, err := s.Eligibility.CheckEligibility(ctx, inst.TenantID, userID, task.DepartmentID, level)
	if err != nil {
		return fmt.Errorf("check eligibility: %w", err)
	}
	if !eligible {
		return port.ErrAssigneeIneligible
	}
	return nil
}

func (s *TaskService) precheckTaskAction(ctx context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*domain.Task, error) {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	if err := checkHumanActionable(task); err != nil {
		return nil, err
	}
	if task.RecordVersion != recordVersion {
		return nil, staleTaskVersion()
	}
	if err := checkTaskActive(task); err != nil {
		return nil, err
	}
	if _, err := s.currentAssignee(ctx, tenantID, task, userID); err != nil {
		return nil, err
	}
	if err := s.checkActorEligible(ctx, tenantID, task, userID); err != nil {
		return nil, err
	}
	return task, nil
}

func checkTaskActive(task *domain.Task) error {
	if task.Status != domain.TaskStatusReady && task.Status != domain.TaskStatusInProgress {
		return port.ErrInvalidTaskState
	}
	return nil
}

func (s *TaskService) OverrideAssignee(ctx context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
	task, err := s.Tasks.GetByInstanceAndNode(ctx, in.TenantID, in.InstanceID, in.NodeKey)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	if err := checkHumanActionable(task); err != nil {
		return nil, err
	}
	if err := checkTaskActive(task); err != nil {
		return nil, port.ErrNodeAlreadyResolved
	}
	if task.RecordVersion != in.RecordVersion {
		return nil, staleTaskVersion()
	}

	assignments, err := s.Assignments.ListActiveByTask(ctx, in.TenantID, task.ID)
	if err != nil {
		return nil, err
	}
	previousUserID := replacedUser(task, assignments)
	if previousUserID == in.NewUserID {
		return nil, port.ErrOverrideNoOp
	}
	inst, err := s.Instances.GetByID(ctx, in.TenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	if err := checkInstanceActionable(inst); err != nil {
		return nil, err
	}
	if err := s.checkEligible(ctx, inst, task, in.NewUserID); err != nil {
		return nil, err
	}

	override, version, err := s.storeOverride(ctx, in, inst, task, previousUserID)
	if err != nil {
		return nil, err
	}
	return toPortAssigneeOverride(override, version), nil
}

// storeOverride bumps the task's record_version, stores the row and queues the reassign signal in one
// transaction. Of two admins overriding the same task only the first persists, and once the row is
// committed the relay delivers the move even if Temporal is down. It returns the version the task now has.
func (s *TaskService) storeOverride(ctx context.Context, in port.AssigneeOverrideInput, inst *domain.Instance, task *domain.Task, previousUserID uuid.UUID) (*domain.AssigneeOverride, int64, error) {
	override := &domain.AssigneeOverride{
		ID: uuid.New(), TenantID: in.TenantID, WorkflowInstanceID: in.InstanceID, NodeKey: in.NodeKey,
		PreviousUserID: domain.OptionalUserID(previousUserID), NewUserID: in.NewUserID, Reason: in.Reason, ActorUserID: in.ActorUserID,
	}
	var bumped *domain.Task
	err := s.Transactor.RunInTx(withTenantGUC(ctx, in.TenantID), func(ctx context.Context) error {
		var err error
		if bumped, err = s.Tasks.BumpRecordVersion(ctx, in.TenantID, task.ID, task.RecordVersion); err != nil {
			return wrapTaskErr(err)
		}
		if err := s.Overrides.Create(ctx, override); err != nil {
			return err
		}
		return recordWorkflowSignal(ctx, s.Signals, inst, port.SignalInstanceReassign, reassignSignalWire{
			TaskID: task.ID.String(), OldUserID: previousUserWire(previousUserID), NewUserID: in.NewUserID.String(),
			AdminUserID: in.ActorUserID.String(), RecordVersion: bumped.RecordVersion,
		})
	})
	if err != nil {
		return nil, 0, err
	}
	return override, bumped.RecordVersion, nil
}
