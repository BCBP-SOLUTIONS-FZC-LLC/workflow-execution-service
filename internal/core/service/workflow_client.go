package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.WorkflowClient = (*WorkflowClient)(nil)

var errEligibilityRead = errors.New("eligibility read failed")

type WorkflowClient struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Temporal    port.TemporalClient
	Eligibility port.EligibilityChecker
	Definitions port.DefinitionServiceClient
	Log         port.Logger
}

func (s *WorkflowClient) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func delegationReasonTag(delegationID uuid.UUID) string {
	return "delegation:" + delegationID.String()
}

func matchingAssignments(assignments []*domain.TaskAssignment, delegationID *uuid.UUID) []*domain.TaskAssignment {
	if delegationID == nil {
		return assignments
	}
	tag := delegationReasonTag(*delegationID)
	var out []*domain.TaskAssignment
	for _, a := range assignments {
		if strings.HasPrefix(a.Reason, tag) {
			out = append(out, a)
		}
	}
	return out
}

func (s *WorkflowClient) ReassignDelegate(ctx context.Context, in port.ReassignDelegateInput) (int, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.OldDelegateID)
	if err != nil {
		return 0, err
	}
	matched := matchingAssignments(active, in.DelegationID)

	plans := newCompiledPlanCache(s.Definitions, s.logger())
	answered := map[eligibilityPair]bool{}
	count := 0
	for _, a := range matched {
		task, err := s.Tasks.GetByID(ctx, in.TenantID, a.TaskID)
		if err != nil {
			s.logger().Warn("skipping assignment with unreadable task", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		eligible, err := s.checkAssigneeEligible(ctx, plans, answered, in.TenantID, task, in.NewDelegateID)
		if errors.Is(err, errEligibilityRead) {
			return count, err
		}
		if err != nil {
			s.logger().Warn("skipping assignment: eligibility check failed", map[string]any{"task_id": task.ID, "error": err.Error()})
			continue
		}
		if !eligible {
			s.logger().Warn("skipping assignment: new delegate ineligible for task", map[string]any{"task_id": task.ID})
			continue
		}
		if _, err := s.Assignments.Vacate(ctx, in.TenantID, a.ID); err != nil {
			s.logger().Warn("failed to vacate assignment during delegate reassignment", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		newAssignment := &domain.TaskAssignment{
			ID: uuid.New(), TenantID: in.TenantID, TaskID: a.TaskID,
			UserID: in.NewDelegateID, Reason: a.Reason, IsLead: a.IsLead,
		}
		if err := s.Assignments.Create(ctx, newAssignment); err != nil {
			s.logger().Warn("failed to create replacement assignment during delegate reassignment", map[string]any{"task_id": a.TaskID, "error": err.Error()})
			continue
		}
		if err := s.signalReassignForTask(ctx, task, a.UserID, in.NewDelegateID); err != nil {
			s.logger().Warn("failed to signal instance-reassign, DB state already updated", map[string]any{"task_id": a.TaskID, "error": err.Error()})
		}
		count++
	}
	return count, nil
}

func (s *WorkflowClient) CancelByDelegate(ctx context.Context, in port.CancelByDelegateInput) (int, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateUserID)
	if err != nil {
		return 0, err
	}
	matched := matchingAssignments(active, in.DelegationID)

	count := 0
	for _, a := range matched {
		if _, err := s.Assignments.Vacate(ctx, in.TenantID, a.ID); err != nil {
			s.logger().Warn("failed to vacate assignment during delegate cancellation", map[string]any{"assignment_id": a.ID, "error": err.Error()})
			continue
		}
		count++
	}
	return count, nil
}

func (s *WorkflowClient) DelegateImpact(ctx context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
	active, err := s.Assignments.ListActiveByUser(ctx, in.TenantID, in.DelegateUserID)
	if err != nil {
		return port.DelegateImpactResult{}, err
	}
	matched := matchingAssignments(active, in.DelegationID)
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

func (s *WorkflowClient) checkAssigneeEligible(ctx context.Context, plans *compiledPlanCache, answered map[eligibilityPair]bool, tenantID uuid.UUID, task *domain.Task, newUserID uuid.UUID) (bool, error) {
	if s.Eligibility == nil || s.Definitions == nil {
		return true, nil
	}
	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return false, err
	}
	plan, err := plans.mainPlan(ctx, tenantID, inst.WorkflowVersionID)
	if err != nil {
		return false, err
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
		return false, fmt.Errorf("%w: %w", errEligibilityRead, err)
	}
	answered[pair] = eligible
	return eligible, nil
}

func (s *WorkflowClient) signalReassignForTask(ctx context.Context, task *domain.Task, oldUserID, newUserID uuid.UUID) error {
	inst, err := s.Instances.GetByID(ctx, task.TenantID, task.WorkflowInstanceID)
	if err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceReassign, reassignSignalWire{
		TaskID: task.ID.String(), OldUserID: oldUserID.String(), NewUserID: newUserID.String(),
		RecordVersion: task.RecordVersion,
	})
}
