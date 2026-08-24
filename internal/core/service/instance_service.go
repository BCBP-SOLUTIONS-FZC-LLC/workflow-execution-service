package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// withTenantGUC sets the app.tenant_id GUC on ctx before a RunInTx/
// RunInTxWithRetry call that composes multiple repo calls — Transactor
// acquires its connection using this ctx before the callback ever runs, so
// a repo method's own per-call GUC re-assertion from inside the callback
// comes too late (internal/adapter/outbound/postgres/base.go's own doc
// comment, restated here since internal/core/service cannot import that
// package — arch-lint's adapter/service dependency direction).
func withTenantGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID.String()})
}

var _ port.InstanceService = (*InstanceService)(nil)

// InstanceService implements port.InstanceService (LLD §5.1-§5.10): the six
// lifecycle-signal methods (Pause/Resume/Cancel/ForceForward/ForceBack plus
// Terminate) validate then either forward a signal or, for Terminate, write
// terminal DB state directly before calling TemporalClient.TerminateWorkflow
// (LLD §3.1 — the one non-signal client call).
type InstanceService struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Outbox      port.OutboxRepository
	Transactor  port.Transactor
	Temporal    port.TemporalClient
	Definitions port.DefinitionServiceClient
	Eligibility port.EligibilityChecker
	Validator   port.EventValidator
	// Cache is nil-safe (matching handler.Services' own Cache convention):
	// Start's compiled-plan read no-ops straight to Definitions when nil.
	Cache port.CacheStore
	Log   port.Logger
}

func (s *InstanceService) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

// deptUUID derives a placeholder IAM department UUID from a compiled plan's
// display-slug department ID (execution_service LLD §4.3: reading a real one
// out of a BPMN lane's extensionElements is a still-open definition_service
// compiler TODO). Mirrors internal/adapter/outbound/temporal/helpers.go's own
// deptUUID byte-for-byte — duplicated, not imported, since this package
// cannot depend on that one (arch-lint's adapter/service dependency
// direction). Both sides must derive the same value for the same deptID for
// this to mean anything once eligibility checks against a real IAM
// department ID land; until then, this is exactly as functionally
// meaningless as the Worker-side copy, by the same already-accepted,
// tracked gap.
func deptUUID(deptID string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:department:"+deptID))
}

// stageNodeKey mirrors internal/workflow/stage.go's own unexported function
// byte-for-byte (same duplication reason as deptUUID above): dept+NodeID
// when NodeID is populated, else dept+Type.
func stageNodeKey(deptID string, stage *dsl.StageDef) string {
	if stage.NodeID != "" {
		return deptID + "/" + stage.NodeID
	}
	return deptID + "/" + stage.Type
}

func findPlan(collab *dsl.CompiledCollaboration, name string) *dsl.CompiledPlan {
	for _, p := range collab.Plans {
		if p.Name == name {
			return p
		}
	}
	return nil
}

// fetchCompiledWorkflow is Start's cache-aside read: a hit populated by
// TemplateCachePrewarmer skips the Definitions round-trip entirely; a miss,
// a cache error, or an unmarshalable cached value all fall through to the
// direct GetCompiledWorkflow call unchanged — the cache is a latency
// optimization only, never Start's source of truth.
func (s *InstanceService) fetchCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if s.Cache != nil {
		raw, err := s.Cache.Get(ctx, compiledPlanCacheKey(tenantID, versionID))
		if err != nil {
			s.logger().Warn("compiled-plan cache read failed, falling back to Definitions", map[string]any{"error": err.Error()})
		} else if raw != "" {
			var compiled port.CompiledWorkflow
			if err := json.Unmarshal([]byte(raw), &compiled); err == nil {
				return &compiled, nil
			}
			s.logger().Warn("compiled-plan cache hit failed to unmarshal, falling back to Definitions", nil)
		}
	}
	return s.Definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
}

func (s *InstanceService) Start(ctx context.Context, in port.StartInstanceInput) (*port.Instance, error) {
	compiled, err := s.fetchCompiledWorkflow(ctx, in.TenantID, in.WorkflowVersionID)
	if err != nil {
		return nil, err
	}
	if compiled.Status != "PUBLISHED" {
		return nil, port.ErrVersionNotPublished
	}
	if !compiled.IsValid {
		return nil, port.ErrVersionInvalid
	}

	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(compiled.CompiledPlanJSON), &collab); err != nil {
		return nil, fmt.Errorf("unmarshal compiled plan: %w", err)
	}
	mainPlan := findPlan(&collab, collab.MainPlan)
	if mainPlan == nil {
		return nil, fmt.Errorf("main plan %q not found in compiled collaboration", collab.MainPlan)
	}

	if err := validateOverrideMap(mainPlan, in.OverrideMap); err != nil {
		return nil, err
	}

	if err := s.validateAssigneeEligibility(ctx, mainPlan, in.OverrideMap); err != nil {
		return nil, err
	}

	overrideMapJSON, err := json.Marshal(overrideMapToStrings(in.OverrideMap))
	if err != nil {
		return nil, fmt.Errorf("marshal override_map: %w", err)
	}

	instanceID := uuid.New()
	temporalWorkflowID := in.TenantID.String() + ":" + in.BusinessKey
	inst := &domain.Instance{
		ID:                 instanceID,
		TenantID:           in.TenantID,
		WorkflowID:         compiled.WorkflowID,
		WorkflowVersionID:  in.WorkflowVersionID,
		BusinessKey:        in.BusinessKey,
		TemporalWorkflowID: temporalWorkflowID,
		Status:             domain.InstanceStatusRunning,
		CurrentNodeKeys:    []string{},
		ContextJSON:        in.ContextJSON,
		OverrideMap:        overrideMapJSON,
		TaskQueue:          mainPlan.TaskQueue,
		StartedByUserID:    in.StartedByUserID,
	}

	txErr := s.Transactor.RunInTx(withTenantGUC(ctx, in.TenantID), func(ctx context.Context) error {
		if err := s.Instances.Create(ctx, inst); err != nil {
			return wrapInstanceErr(err)
		}
		sink := instanceEventSink{Outbox: s.Outbox, Validator: s.Validator}
		return sink.enqueueInstanceEvent(ctx, in.TenantID.String(), domain.EventWorkflowInstanceStarted, inst.ID.String(),
			in.StartedByUserID.String(), domain.NewWorkflowInstanceStartedPayload(instanceCore(inst), in.StartedByUserID))
	})
	if txErr != nil {
		return nil, txErr
	}

	// StartWorkflow happens outside the DB transaction — a Temporal call
	// inside a transaction that then rolled back would leave a started
	// workflow with no matching row, the opposite (and worse) failure mode.
	// A bounded retry absorbs the common transient case (a frontend blip in
	// the narrow window between commit and this call); persistent failure
	// past that is logged and returned, an accepted residual matching
	// Terminate's own post-commit-RPC-failure gap, since the
	// (tenant_id, business_key) unique constraint means a plain client retry
	// of POST /instances can't recover this instance on its own.
	startErr := s.startWorkflowWithRetry(ctx, port.StartWorkflowInput{
		TemporalWorkflowID: temporalWorkflowID,
		TaskQueue:          mainPlan.TaskQueue,
		TenantID:           in.TenantID,
		InstanceID:         instanceID,
		WorkflowVersionID:  in.WorkflowVersionID,
		ContextJSON:        string(in.ContextJSON),
		OverrideMap:        overrideMapToStrings(in.OverrideMap),
	})
	if startErr != nil {
		s.logger().Error("StartWorkflow failed after workflow_instance was committed", map[string]any{
			"instance_id": instanceID, "tenant_id": in.TenantID, "business_key": in.BusinessKey, "error": startErr.Error(),
		})
		return nil, fmt.Errorf("start workflow: %w", startErr)
	}

	return toPortInstance(inst), nil
}

const startWorkflowMaxAttempts = 3

func (s *InstanceService) startWorkflowWithRetry(ctx context.Context, in port.StartWorkflowInput) error {
	var lastErr error
	for attempt := 1; attempt <= startWorkflowMaxAttempts; attempt++ {
		if _, err := s.Temporal.StartWorkflow(ctx, in); err != nil {
			lastErr = err
			if attempt < startWorkflowMaxAttempts {
				select {
				case <-ctx.Done():
					return fmt.Errorf("start workflow retry: %w", ctx.Err())
				case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}

// validateOverrideMap rejects an override_map entry whose node key doesn't
// exist in the compiled plan (LLD §5.10's OVERRIDE_MAP_INVALID).
func validateOverrideMap(plan *dsl.CompiledPlan, overrideMap map[string]uuid.UUID) error {
	if len(overrideMap) == 0 {
		return nil
	}
	valid := make(map[string]struct{})
	for _, dept := range plan.Departments {
		for i := range dept.Stages {
			valid[stageNodeKey(dept.ID, &dept.Stages[i])] = struct{}{}
		}
	}
	for nodeKey := range overrideMap {
		if _, ok := valid[nodeKey]; !ok {
			return fmt.Errorf("%w: node key %q", port.ErrOverrideMapInvalid, nodeKey)
		}
	}
	return nil
}

// validateAssigneeEligibility is LLD §5.5's bulk default-assignee
// re-validation: every node the compiled plan resolves to an assignee
// (default, or an override_map entry replacing it — resolveAssignees'
// override-replaces-default rule, mirrored here) gets its effective
// assignee(s) checked, not only nodes named in override_map. Connector-typed
// stages are skipped — they have no human assignee, ever. Walks
// plan.Departments directly (a flat list shared by every ExecutionStep
// regardless of Sequential/Parallel/Exclusive/SubWorkflow nesting depth), so
// this already covers every node reachable under any nested gateway without
// needing to separately walk the execution graph itself.
//
// Note for future revisit: no attempt is made to cap or batch-limit the
// number of distinct (department, level) pairs a pathological large plan
// could produce; internal/core/service/instance_service_test.go's own
// bulk-eligibility tests should grow a case here if real-world call volume
// ever becomes a concern (LLD §6.7).
func (s *InstanceService) validateAssigneeEligibility(ctx context.Context, plan *dsl.CompiledPlan, overrideMap map[string]uuid.UUID) error {
	type check struct {
		nodeKey string
		req     port.EligibilityCheckRequest
	}
	var checks []check

	for _, dept := range plan.Departments {
		if dept.Ignore {
			continue
		}
		for i := range dept.Stages {
			stage := &dept.Stages[i]
			if stage.ConnectorType != "" {
				continue
			}
			nodeKey := stageNodeKey(dept.ID, stage)

			var userIDs []uuid.UUID
			if override, ok := overrideMap[nodeKey]; ok {
				userIDs = []uuid.UUID{override}
			} else {
				for _, raw := range stage.DefaultAssignees {
					id, err := uuid.Parse(raw)
					if err != nil {
						continue // not this method's job to validate DSL well-formedness
					}
					userIDs = append(userIDs, id)
				}
			}

			for _, userID := range userIDs {
				checks = append(checks, check{
					nodeKey: nodeKey,
					req: port.EligibilityCheckRequest{
						NewUserID: userID, DepartmentID: deptUUID(dept.ID), RequiredLevel: stage.Role,
					},
				})
			}
		}
	}

	if len(checks) == 0 {
		return nil
	}

	requests := make([]port.EligibilityCheckRequest, len(checks))
	for i, c := range checks {
		requests[i] = c.req
	}
	results, err := s.Eligibility.CheckEligibilityBatch(ctx, requests, uuid.Nil)
	if err != nil {
		return err
	}

	var ineligibleNodes []string
	for i, eligible := range results {
		if !eligible {
			ineligibleNodes = append(ineligibleNodes, checks[i].nodeKey)
		}
	}
	if len(ineligibleNodes) > 0 {
		return &port.AssigneeIneligibleError{Nodes: ineligibleNodes}
	}
	return nil
}

func overrideMapToStrings(m map[string]uuid.UUID) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.String()
	}
	return out
}

func (s *InstanceService) List(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
	repoFilter := port.InstanceListFilter{
		WorkflowVersionID: filter.WorkflowVersionID,
		StartedAfter:      filter.StartedAfter,
		StartedBefore:     filter.StartedBefore,
	}
	if filter.Status != nil {
		status := domain.InstanceStatus(*filter.Status)
		repoFilter.Status = &status
	}

	rows, next, err := s.Instances.ListByTenant(ctx, tenantID, repoFilter, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.Instance]{}, wrapInstanceErr(err)
	}
	items := make([]*port.Instance, len(rows))
	for i, inst := range rows {
		items[i] = toPortInstance(inst)
	}
	return port.PageResult[*port.Instance]{Items: items, NextCursor: encodeNextCursor(next)}, nil
}

func (s *InstanceService) Get(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope) (*port.Instance, []*port.Task, error) {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return nil, nil, wrapInstanceErr(err)
	}
	tasks, _, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
	if err != nil {
		return nil, nil, wrapTaskErr(err)
	}

	if !scope.IsAdmin {
		if !s.callerInScope(ctx, tenantID, scope, tasks) {
			return nil, nil, port.ErrNotAuthorizedForRead
		}
	}

	portTasks := make([]*port.Task, len(tasks))
	for i, t := range tasks {
		portTasks[i] = toPortTask(t)
	}
	return toPortInstance(inst), portTasks, nil
}

// callerInScope implements the intra-tenant visibility check (LLD §9.2): the
// caller may see this instance if they're a current department match or an
// active assignee on any of its current tasks. Checking every current
// task's active assignments is bounded by how many tasks can be
// simultaneously active (parallel-gateway branches), typically small.
func (s *InstanceService) callerInScope(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, tasks []*domain.Task) bool {
	for _, t := range tasks {
		for _, d := range scope.Departments {
			if d.DepartmentID == t.DepartmentID {
				return true
			}
		}
		assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, t.ID)
		if err != nil {
			continue
		}
		for _, a := range assignments {
			if a.UserID == scope.CallerUserID {
				return true
			}
		}
	}
	return false
}

func (s *InstanceService) ListEvents(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, page port.Page) (port.PageResult[*port.WorkflowEvent], error) {
	if !scope.IsAdmin {
		tasks, _, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
		if err != nil {
			return port.PageResult[*port.WorkflowEvent]{}, wrapTaskErr(err)
		}
		if !s.callerInScope(ctx, tenantID, scope, tasks) {
			return port.PageResult[*port.WorkflowEvent]{}, port.ErrNotAuthorizedForRead
		}
	}

	rows, next, err := s.Outbox.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.WorkflowEvent]{}, err
	}
	items := make([]*port.WorkflowEvent, 0, len(rows))
	for _, rec := range rows {
		event, err := toWorkflowEvent(rec, tenantID, instanceID)
		if err != nil {
			s.logger().Warn("skipping unrenderable outbox event", map[string]any{"event_id": rec.ID, "error": err.Error()})
			continue
		}
		items = append(items, event)
	}
	return port.PageResult[*port.WorkflowEvent]{Items: items, NextCursor: encodeNextCursor(next)}, nil
}

// adminSignalWire mirrors internal/workflow/signals.go's own unexported
// adminSignal field-for-field (same duplication reason as deptUUID/
// stageNodeKey above) — Temporal's default JSON data converter matches
// payload fields by name, not type identity, so this only stays correct as
// long as the two structs' field names agree; the
// testsuite.WorkflowTestSuite round-trip test in test/workflow/ exists
// specifically to catch drift.
type adminSignalWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetDeptID  string
	TargetNodeKey string
	RecordVersion int64
}

// signalAnnotation bundles a signal's two free-text-ish fields — reason is a
// genuine human-supplied text (Pause/Cancel's own API-caller-facing reason);
// initiator is a caller-computed tag identifying what triggered the signal
// (domain.InitiatorAdmin, InitiatorTenantState, InitiatorOOO, InitiatorSafetyNet)
// so a pause/resume caused by something other than a direct admin API call
// reports its real cause on the wire instead of a hardcoded "admin".
type signalAnnotation struct {
	reason    string
	initiator string
}

func (s *InstanceService) signalLifecycle(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, ann signalAnnotation, recordVersion int64, signalName string) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if inst.RecordVersion != recordVersion {
		return port.ErrRecordVersionConflict
	}
	if err := validateInstanceStateFor(signalName, inst.Status); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, signalName, adminSignalWire{
		AdminUserID: actorUserID.String(), Reason: ann.reason, Initiator: ann.initiator, RecordVersion: recordVersion,
	})
}

// validateInstanceStateFor mirrors internal/workflow/signals.go's own
// signalPreconditions table (the same duplication category as adminSignalWire
// above) — the synchronous HTTP-layer pre-check LLD §5.10 requires ahead of
// the in-workflow Activity's own authoritative check.
func validateInstanceStateFor(signalName string, status domain.InstanceStatus) error {
	var allowed []domain.InstanceStatus
	switch signalName {
	case port.SignalInstancePause:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning}
	case port.SignalInstanceResume:
		allowed = []domain.InstanceStatus{domain.InstanceStatusPaused}
	case port.SignalInstanceCancel:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded}
	case port.SignalInstanceForceFwd, port.SignalInstanceForceBack:
		allowed = []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded}
	}
	for _, a := range allowed {
		if a == status {
			return nil
		}
	}
	if status == domain.InstanceStatusCompleted || status == domain.InstanceStatusTerminated || status == domain.InstanceStatusFailed {
		return port.ErrInstanceAlreadyTerminal
	}
	return port.ErrInvalidInstanceState
}

func (s *InstanceService) Pause(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{reason: reason, initiator: domain.InitiatorAdmin}, recordVersion, port.SignalInstancePause)
}

func (s *InstanceService) Resume(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{initiator: domain.InitiatorAdmin}, recordVersion, port.SignalInstanceResume)
}

func (s *InstanceService) Cancel(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{reason: reason}, recordVersion, port.SignalInstanceCancel)
}

func (s *InstanceService) ForceForward(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if inst.RecordVersion != recordVersion {
		return port.ErrRecordVersionConflict
	}
	if err := validateInstanceStateFor(port.SignalInstanceForceFwd, inst.Status); err != nil {
		return err
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceForceFwd, adminSignalWire{
		AdminUserID: actorUserID.String(), TargetNodeKey: targetNodeKey, RecordVersion: recordVersion,
	})
}

func (s *InstanceService) ForceBack(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
	return s.signalLifecycle(ctx, tenantID, instanceID, actorUserID, signalAnnotation{}, recordVersion, port.SignalInstanceForceBack)
}

// Terminate is the one direct (non-signal) client call (LLD §3.1) — no
// record_version, not signal-validated. The service writes terminal DB
// state itself (instance TERMINATED, active tasks FAILED, their assignments
// vacated) inside one retrying transaction, then calls TerminateWorkflow;
// a failure of that last RPC after the transaction commits is an accepted,
// documented residual (DB truth is already correct either way).
func (s *InstanceService) Terminate(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if inst.Status == domain.InstanceStatusCompleted || inst.Status == domain.InstanceStatusTerminated || inst.Status == domain.InstanceStatusFailed {
		return port.ErrInstanceAlreadyTerminal
	}

	txErr := s.Transactor.RunInTxWithRetry(withTenantGUC(ctx, tenantID), func(ctx context.Context) error {
		updated, err := s.Instances.UpdateStatus(ctx, tenantID, instanceID, domain.InstanceStatusTerminated, inst.RecordVersion)
		if err != nil {
			return wrapInstanceErr(err)
		}
		if err := cascadeFailTasksForInstance(ctx, s.Tasks, s.Assignments, tenantID, instanceID); err != nil {
			return err
		}
		inst = updated
		sink := instanceEventSink{Outbox: s.Outbox, Validator: s.Validator}
		return sink.enqueueInstanceEvent(ctx, tenantID.String(), domain.EventWorkflowInstanceTerminated, instanceID.String(),
			actorUserID.String(), domain.NewWorkflowInstanceTerminatedPayload(instanceCore(inst), inst.StartedByUserID, domain.TerminatedInitiatorAdmin, &actorUserID))
	})
	if txErr != nil {
		return txErr
	}

	if err := s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, reason); err != nil {
		s.logger().Error("TerminateWorkflow failed after DB state already committed TERMINATED", map[string]any{
			"instance_id": instanceID, "tenant_id": tenantID, "error": err.Error(),
		})
		return fmt.Errorf("terminate workflow: %w", err)
	}
	return nil
}

// cascadeFailTasksForInstance marks every READY/IN_PROGRESS task on
// instanceID FAILED and vacates their active assignments — the shared
// terminal-cascade step both InstanceService.Terminate (single,
// admin-driven) and TenantLifecycleReconciler's tenant-offboard sweep
// (bulk, system-driven) need.
func cascadeFailTasksForInstance(ctx context.Context, tasks port.TaskRepository, assignments port.TaskAssignmentRepository, tenantID, instanceID uuid.UUID) error {
	rows, _, err := tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
	if err != nil {
		return wrapTaskErr(err)
	}
	for _, t := range rows {
		if t.Status != domain.TaskStatusReady && t.Status != domain.TaskStatusInProgress {
			continue
		}
		if _, err := tasks.UpdateStatus(ctx, tenantID, t.ID, domain.TaskStatusFailed, t.RecordVersion); err != nil {
			return wrapTaskErr(err)
		}
		active, err := assignments.ListActiveByTask(ctx, tenantID, t.ID)
		if err != nil {
			return err
		}
		for _, a := range active {
			if _, err := assignments.Vacate(ctx, tenantID, a.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func instanceCore(inst *domain.Instance) domain.CommonCore {
	return domain.CommonCore{WorkflowInstanceID: inst.ID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
}

// instanceEventSink is the shared Outbox+Validator pair behind
// enqueueInstanceEvent — a small struct instead of two extra parameters, so
// both InstanceService and TenantLifecycleReconciler's own tenant-offboard
// termination sweep can build one inline and share the same helper without
// tripping the too-many-parameters lint rule.
type instanceEventSink struct {
	Outbox    port.OutboxRepository
	Validator port.EventValidator
}

// enqueueInstanceEvent builds the "instances/{instanceID}" subject itself
// (LLD §6.4's envelope shape) rather than trusting each call site to format
// it — internal/adapter/outbound/temporal's own equivalent call site follows
// the same convention.
func (s instanceEventSink) enqueueInstanceEvent(ctx context.Context, tenantID, eventType, instanceID, actor string, payload any) error {
	envelope, err := BuildEnvelope(ctx, s.Validator, eventType, tenantID, "instances/"+instanceID, actor, payload)
	if err != nil {
		return err
	}
	return s.Outbox.Enqueue(ctx, envelope)
}

func pageAfter(page port.Page) *port.Cursor {
	if page.Cursor == nil {
		return nil
	}
	return &port.Cursor{CreatedAt: page.Cursor.CreatedAt, ID: page.Cursor.ID}
}

func encodeNextCursor(c *port.Cursor) string {
	if c == nil {
		return ""
	}
	return port.EncodeCursor(port.CursorPosition{CreatedAt: c.CreatedAt, ID: c.ID})
}

// noopLogger is the zero-value fallback used when a caller's Logger
// dependency is nil.
type noopLogger struct{}

func (noopLogger) Debug(string, map[string]any) { /* no-op fallback */ }
func (noopLogger) Info(string, map[string]any)  { /* no-op fallback */ }
func (noopLogger) Warn(string, map[string]any)  { /* no-op fallback */ }
func (noopLogger) Error(string, map[string]any) { /* no-op fallback */ }
