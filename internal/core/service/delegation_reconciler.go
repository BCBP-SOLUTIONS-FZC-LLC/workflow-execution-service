package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

var _ port.DelegationReconciler = (*DelegationReconciler)(nil)

// DelegationReconciler implements port.DelegationReconciler (LLD §6.2 items
// 1-2): DelegationStarted/DelegationEnded's scope-filtered bulk
// reroute/reversal. Shares WorkflowClient's establish-then-signal shape and
// its delegationReasonTag/matchingAssignments-style tag convention, but adds
// the real scope derivation and per-row eligibility re-check that
// WorkflowClient's own HTTP-triggered methods skip — those act on a set an
// earlier Reroute call already scope-derived and eligibility-vetted (LLD
// §5.8's `delegation_id` tag fallback exists precisely so WorkflowClient
// never has to redo that work).
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

// resolveCandidates hydrates each active assignment's task and instance —
// both needed by every caller here (scope-filtering, eligibility's compiled-
// plan lookup, and the post-commit signal loop), unlike WorkflowClient's
// lazier per-matched-row-only instance fetch. An unreadable task or
// instance is logged and skipped, never aborts the batch.
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

// scopeMatches is DelegationStarted's own scope-filter step (LLD §5.8 item
// 2): "all" passes everything, "department" compares the task's own
// department_id, anything else (business-key-scoped domains, "tender"
// today) falls through to the instance's business_key — a single default
// branch rather than a hardcoded literal case, so a future business-key
// scope value needs no code change here.
func scopeMatches(c delegationCandidate, scope string, scopeID *string) bool {
	switch scope {
	case "all":
		return true
	case "department":
		return scopeID != nil && c.task.DepartmentID.String() == *scopeID
	default:
		return scopeID != nil && c.instance.BusinessKey == *scopeID
	}
}

// compiledPlanCache memoizes GetCompiledWorkflow-and-unmarshal per workflow
// version within one Reroute/Reverse call — many candidate rows commonly
// share the same instance or version, and re-fetching the same compiled
// plan per row would multiply Definition Service round-trips for no reason.
type compiledPlanCache struct {
	definitions port.DefinitionServiceClient
	byVersion   map[uuid.UUID]*dsl.CompiledPlan
}

func newCompiledPlanCache(definitions port.DefinitionServiceClient) *compiledPlanCache {
	return &compiledPlanCache{definitions: definitions, byVersion: make(map[uuid.UUID]*dsl.CompiledPlan)}
}

func (c *compiledPlanCache) mainPlan(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	if plan, ok := c.byVersion[versionID]; ok {
		return plan, nil
	}
	compiled, err := c.definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(compiled.CompiledPlanJSON), &collab); err != nil {
		return nil, fmt.Errorf("unmarshal compiled plan: %w", err)
	}
	plan := findPlan(&collab, collab.MainPlan)
	if plan == nil {
		return nil, fmt.Errorf("main plan %q not found in compiled collaboration", collab.MainPlan)
	}
	c.byVersion[versionID] = plan
	return plan, nil
}

// requiredLevelForTask finds the compiled-plan stage backing task's own
// (department_id, node_key) — the reverse of InstanceService.Start's own
// validateAssigneeEligibility traversal, since a task row carries the
// already-resolved deptUUID/NodeKey rather than a plan-slug the way Start's
// own DSL walk does.
func requiredLevelForTask(plan *dsl.CompiledPlan, task *domain.Task) (string, bool) {
	for _, dept := range plan.Departments {
		if deptUUID(&dept) != task.DepartmentID {
			continue
		}
		for i := range dept.Stages {
			stage := &dept.Stages[i]
			if stageNodeKey(dept.ID, stage) == task.NodeKey {
				return stage.Role, true
			}
		}
	}
	return "", false
}

// eligibleCandidates runs the shared batched-eligibility-recheck step (LLD
// §6.2 items 1.3/2.2): resolves each candidate's compiled-plan stage role,
// batches one CheckEligibilityBatch call for candidateUserID against every
// resolved (department_id, level) pair, and returns only the rows that come
// back eligible. A candidate whose plan or stage can't be resolved is
// treated the same as an ineligible one — held, flagged, logged.
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

// commitReassignments is the shared "commit one bulk transaction ... then
// signal each affected instance in a loop" step (LLD §6.2 items 1.4-1.5,
// 2.3-2.4) — the transaction covers vacate+create+outbox-enqueue only; the
// signal loop deliberately runs after commit, outside the transaction,
// mirroring InstanceService.Start's own post-commit StartWorkflow call.
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

	scoped := make([]delegationCandidate, 0, len(candidates))
	for _, c := range candidates {
		if scopeMatches(c, in.Scope, in.ScopeID) {
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
