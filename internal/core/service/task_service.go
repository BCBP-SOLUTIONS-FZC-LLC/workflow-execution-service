package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TaskService = (*TaskService)(nil)

// TaskService implements port.TaskService (LLD §5.4, §5.6, §5.10). Every
// mutating method performs its own synchronous record_version/state
// pre-check before forwarding a signal — the same pattern InstanceService
// uses (LLD §5.10's "synchronous delivery of signal-detected 409s").
type TaskService struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Overrides   port.AssigneeOverrideRepository
	Temporal    port.TemporalClient
	IAM         port.IAMClient
	Log         port.Logger
}

func (s *TaskService) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

// reassignSignalWire mirrors internal/workflow/signals.go's own unexported
// reassignSignal field-for-field (same duplication reason as
// instance_service.go's adminSignalWire).
type reassignSignalWire struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
}

// deptAndSuffix splits a NodeKey ("dept/rest") back into its two halves —
// the inverse of stageNodeKey, matching internal/adapter/outbound/temporal's
// own deptIDFromNodeKey convention (each duplicated locally, arch-lint's
// service/adapter dependency direction forbids importing either).
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

	rows, next, err := s.Tasks.ListByTenant(ctx, tenantID, repoFilter, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.Task]{}, wrapTaskErr(err)
	}
	items := make([]*port.Task, len(rows))
	for i, t := range rows {
		items[i] = toPortTask(t)
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
	return toPortTask(task), portAssignments, nil
}

func (s *TaskService) taskInScope(scope port.ReadScope, task *domain.Task, assignments []*domain.TaskAssignment) bool {
	for _, d := range scope.Departments {
		if d == task.DepartmentID.String() {
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

func (s *TaskService) GetByNode(ctx context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*port.Task, error) {
	task, err := s.Tasks.GetByInstanceAndNode(ctx, tenantID, instanceID, nodeKey)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	return toPortTask(task), nil
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

// checkUserLive is the live pre-check LLD Appendix B documents as
// undesigned. Fails open on any error checking the status (the IAMClient
// contract isn't confirmed with the IAM team yet, see its own doc comment;
// treating "couldn't check" the same as "confirmed unavailable" would make
// every task action fail the moment this dependency has any hiccup) —
// only a successful, confirmed deleted/OOO response rejects the action.
func (s *TaskService) checkUserLive(ctx context.Context, tenantID, userID uuid.UUID) error {
	if s.IAM == nil {
		return nil
	}
	status, err := s.IAM.GetUserStatus(ctx, tenantID, userID)
	if err != nil {
		s.logger().Warn("live user-status check failed, proceeding fail-open", map[string]any{"user_id": userID, "error": err.Error()})
		return nil
	}
	if status.IsDeleted || status.IsOOO {
		return port.ErrAssigneeUnavailable
	}
	return nil
}

// checkHumanActionable rejects a connector-typed task (LLD's own §5.1
// "domain service does not distinguish who or what called it" describes the
// interpreter's signal handling, not whether TaskService's human-facing
// methods should accept a call against a task with zero human assignments,
// ever — they don't).
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
		return nil, port.ErrRecordVersionConflict
	}
	if err := checkTaskActive(task); err != nil {
		return nil, err
	}
	if err := s.checkUserLive(ctx, tenantID, userID); err != nil {
		return nil, err
	}

	assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	var target *domain.TaskAssignment
	for _, a := range assignments {
		if a.IsLead {
			return nil, port.ErrTaskAlreadyClaimed
		}
		if a.UserID == userID {
			target = a
		}
	}
	if target == nil {
		return nil, port.ErrNotAssignee
	}

	if _, err := s.Assignments.SetLead(ctx, tenantID, taskID, target.ID, recordVersion); err != nil {
		return nil, wrapTaskErr(err)
	}
	updated, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	return toPortTask(updated), nil
}

// stageTransitionWire mirrors internal/workflow/signals.go's own unexported
// stageTransitionSignal field-for-field.
type stageTransitionWire struct {
	DeptID        string
	ToStage       string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
	Failed        bool
	Reason        string
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
	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-transition", stageTransitionWire{
		DeptID: deptID, NodeID: nodeID, UserID: userID.String(), ResultJSON: string(resultJSON), RecordVersion: recordVersion,
	}); err != nil {
		return nil, fmt.Errorf("signal stage-transition: %w", err)
	}
	return toPortTask(task), nil
}

// stageDeferWire mirrors internal/workflow/signals.go's own unexported
// stageDeferSignal field-for-field.
type stageDeferWire struct {
	DeptID        string
	FromStage     string
	Reason        string
	UserID        string
	RecordVersion int64
}

func (s *TaskService) Defer(ctx context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*port.Task, error) {
	task, err := s.precheckTaskAction(ctx, tenantID, taskID, userID, recordVersion)
	if err != nil {
		return nil, err
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-defer", stageDeferWire{
		DeptID: deptID, FromStage: nodeID, Reason: reason, UserID: userID.String(), RecordVersion: recordVersion,
	}); err != nil {
		return nil, fmt.Errorf("signal stage-defer: %w", err)
	}
	return toPortTask(task), nil
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
		return nil, port.ErrRecordVersionConflict
	}
	if err := checkTaskActive(task); err != nil {
		return nil, err
	}

	assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	var oldUserID uuid.UUID
	for _, a := range assignments {
		if task.AssigneeMode != "all" || a.IsLead {
			oldUserID = a.UserID
			break
		}
	}
	if oldUserID == newUserID {
		return nil, port.ErrOverrideNoOp
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, wrapInstanceErr(err)
	}
	if err := s.sendReassignSignal(ctx, inst, taskID, oldUserID, newUserID, actorUserID, recordVersion); err != nil {
		return nil, err
	}
	return toPortTask(task), nil
}

func (s *TaskService) sendReassignSignal(ctx context.Context, inst *domain.Instance, taskID, oldUserID, newUserID, actorUserID uuid.UUID, recordVersion int64) error {
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceReassign, reassignSignalWire{
		TaskID: taskID.String(), OldUserID: oldUserID.String(), NewUserID: newUserID.String(),
		AdminUserID: actorUserID.String(), RecordVersion: recordVersion,
	}); err != nil {
		return fmt.Errorf("signal instance-reassign: %w", err)
	}
	return nil
}

// precheckTaskAction is Claim/Complete/Defer's shared version+state+
// assignee+liveness pre-check.
func (s *TaskService) precheckTaskAction(ctx context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*domain.Task, error) {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, wrapTaskErr(err)
	}
	if err := checkHumanActionable(task); err != nil {
		return nil, err
	}
	if task.RecordVersion != recordVersion {
		return nil, port.ErrRecordVersionConflict
	}
	if err := checkTaskActive(task); err != nil {
		return nil, err
	}
	if _, err := s.currentAssignee(ctx, tenantID, task, userID); err != nil {
		return nil, err
	}
	if err := s.checkUserLive(ctx, tenantID, userID); err != nil {
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

// OverrideAssignee performs LLD §5.4 steps 1 (validate + version check) and
// 3 (persist) only — the caller (handler) already ran step 2 (eligibility)
// before invoking this, and fires step 4 (the instance-reassign signal, via
// SignalReassign below) only after this call succeeds. This method writes
// only the assignee_overrides audit row; the actual workflow_task_assignment
// vacate+insert happens asynchronously, worker-side, once the paired signal
// is processed (ReassignAssignmentActivity) — matching the Signal-Only
// pattern's one documented persist-then-signal exception (LLD §5.7).
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
		return nil, port.ErrRecordVersionConflict
	}

	assignments, err := s.Assignments.ListActiveByTask(ctx, in.TenantID, task.ID)
	if err != nil {
		return nil, err
	}
	if len(assignments) == 0 {
		return nil, port.ErrNotAssignee
	}
	previousUserID := assignments[0].UserID
	if previousUserID == in.NewUserID {
		return nil, port.ErrOverrideNoOp
	}

	override := &domain.AssigneeOverride{
		ID: uuid.New(), TenantID: in.TenantID, WorkflowInstanceID: in.InstanceID, NodeKey: in.NodeKey,
		PreviousUserID: previousUserID, NewUserID: in.NewUserID, Reason: in.Reason, ActorUserID: in.ActorUserID,
	}
	if err := s.Overrides.Create(ctx, override); err != nil {
		return nil, err
	}
	return toPortAssigneeOverride(override, task.RecordVersion), nil
}

func (s *TaskService) SignalReassign(ctx context.Context, tenantID, taskID, actorUserID, previousUserID, newUserID uuid.UUID, recordVersion int64) error {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return wrapTaskErr(err)
	}
	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	return s.sendReassignSignal(ctx, inst, taskID, previousUserID, newUserID, actorUserID, recordVersion)
}
