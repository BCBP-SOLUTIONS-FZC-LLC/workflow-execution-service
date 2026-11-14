package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// csdIDCall is a read of one row: the tenant it was scoped to and the id asked for.
type csdIDCall struct {
	TenantID uuid.UUID
	ID       uuid.UUID
}

// csdTaskRepo answers only for the tenant that owns the task, the way
// row-level security does, and records every read.
type csdTaskRepo struct {
	port.TaskRepository
	byID map[uuid.UUID]*domain.Task
	gets []csdIDCall
}

func (r *csdTaskRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.gets = append(r.gets, csdIDCall{tenantID, id})
	task, ok := r.byID[id]
	if !ok || task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return task, nil
}

type csdSetLeadCall struct {
	TenantID      uuid.UUID
	TaskID        uuid.UUID
	AssignmentID  uuid.UUID
	RecordVersion int64
}

// csdAssignmentRepo keeps the real SetLead contract: it checks the task's
// record version, bumps it, and leaves exactly one lead.
type csdAssignmentRepo struct {
	port.TaskAssignmentRepository
	tasks    *csdTaskRepo
	byID     map[uuid.UUID]*domain.TaskAssignment
	order    []uuid.UUID
	listErr  error
	lists    []csdIDCall
	setLeads []csdSetLeadCall
}

func (r *csdAssignmentRepo) add(a *domain.TaskAssignment) {
	r.byID[a.ID] = a
	r.order = append(r.order, a.ID)
}

func (r *csdAssignmentRepo) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lists = append(r.lists, csdIDCall{tenantID, taskID})
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []*domain.TaskAssignment
	for _, id := range r.order {
		a := r.byID[id]
		if a.TenantID == tenantID && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *csdAssignmentRepo) SetLead(_ context.Context, tenantID, taskID, id uuid.UUID, taskRecordVersion int64) (*domain.TaskAssignment, error) {
	r.setLeads = append(r.setLeads, csdSetLeadCall{tenantID, taskID, id, taskRecordVersion})
	task, ok := r.tasks.byID[taskID]
	if !ok || task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != taskRecordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	task.RecordVersion++
	for _, a := range r.byID {
		if a.TaskID == taskID {
			a.IsLead = a.ID == id
		}
	}
	return r.byID[id], nil
}

func (r *csdAssignmentRepo) leadOf(taskID uuid.UUID) uuid.UUID {
	for _, a := range r.byID {
		if a.TaskID == taskID && a.IsLead {
			return a.UserID
		}
	}
	return uuid.Nil
}

type csdInstanceRepo struct {
	port.InstanceRepository
	byID map[uuid.UUID]*domain.Instance
	gets []csdIDCall
}

func (r *csdInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gets = append(r.gets, csdIDCall{tenantID, id})
	inst, ok := r.byID[id]
	if !ok || inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return inst, nil
}

type csdEligibilityCall struct {
	TenantID     uuid.UUID
	UserID       uuid.UUID
	DepartmentID uuid.UUID
	Level        string
}

type csdEligibility struct {
	port.EligibilityChecker
	err   error
	calls []csdEligibilityCall
}

func (e *csdEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, requiredLevel string) (bool, error) {
	e.calls = append(e.calls, csdEligibilityCall{tenantID, userID, departmentID, requiredLevel})
	if e.err != nil {
		return false, e.err
	}
	return true, nil
}

type csdDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []csdIDCall
}

func (d *csdDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, csdIDCall{tenantID, versionID})
	if d.err != nil {
		return nil, d.err
	}
	return d.resp, nil
}

type csdCache struct {
	port.CacheStore
	values map[string]string
	gets   []string
	sets   []string
}

func (c *csdCache) Get(_ context.Context, key string) (string, error) {
	c.gets = append(c.gets, key)
	return c.values[key], nil
}

func (c *csdCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.sets = append(c.sets, key)
	c.values[key] = value
	return nil
}

type csdSignalCall struct {
	TemporalWorkflowID string
	InstanceID         uuid.UUID
	Name               string
}

type csdTemporal struct {
	port.TemporalClient
	signals []csdSignalCall
}

func (c *csdTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, _ any) error {
	c.signals = append(c.signals, csdSignalCall{temporalWorkflowID, instanceID, signalName})
	return nil
}

// csdTaskHarness is one task of department "finance" at node "finance/review"
// with two active assignees (the caller and another user), on a running
// instance whose plan is served by csdDefinitions.
type csdTaskHarness struct {
	svc         *service.TaskService
	tasks       *csdTaskRepo
	assignments *csdAssignmentRepo
	instances   *csdInstanceRepo
	eligibility *csdEligibility
	definitions *csdDefinitions
	cache       *csdCache
	temporal    *csdTemporal

	tenantID, instanceID, versionID, deptID uuid.UUID
	callerID, otherID                       uuid.UUID
	task                                    *domain.Task
	caller, other                           *domain.TaskAssignment
}

const csdTaskVersion = 3

// csdNewTaskHarness builds the harness. assigneeMode is "all" (claim) or
// "single"; stages is what the compiled plan holds for the department.
func csdNewTaskHarness(t *testing.T, assigneeMode string, stages ...dsl.StageDef) *csdTaskHarness {
	t.Helper()
	h := &csdTaskHarness{
		tenantID: uuid.New(), instanceID: uuid.New(), versionID: uuid.New(), deptID: uuid.New(),
		callerID: uuid.New(), otherID: uuid.New(),
	}
	h.task = &domain.Task{
		ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: h.instanceID, NodeKey: "finance/review",
		DepartmentID: h.deptID, Status: domain.TaskStatusReady, AssigneeMode: assigneeMode, RecordVersion: csdTaskVersion,
	}
	h.tasks = &csdTaskRepo{byID: map[uuid.UUID]*domain.Task{h.task.ID: h.task}}
	h.assignments = &csdAssignmentRepo{tasks: h.tasks, byID: map[uuid.UUID]*domain.TaskAssignment{}}
	h.caller = &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: h.task.ID, UserID: h.callerID, IsActive: true}
	h.other = &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: h.task.ID, UserID: h.otherID, IsActive: true}
	h.assignments.add(h.caller)
	h.assignments.add(h.other)
	h.instances = &csdInstanceRepo{byID: map[uuid.UUID]*domain.Instance{h.instanceID: {
		ID: h.instanceID, TenantID: h.tenantID, WorkflowVersionID: h.versionID, TemporalWorkflowID: "wf-" + h.instanceID.String(),
		Status: domain.InstanceStatusRunning,
	}}}
	h.eligibility = &csdEligibility{}
	plan := compiledPlanJSONWithDept(t, dsl.DepartmentDef{ID: "finance", IAMDepartmentID: h.deptID.String(), Stages: stages})
	h.definitions = &csdDefinitions{resp: publishedCompiledWorkflow(uuid.New(), h.versionID, plan)}
	h.cache = &csdCache{values: map[string]string{}}
	h.temporal = &csdTemporal{}
	h.svc = &service.TaskService{
		Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments,
		Temporal: h.temporal, Eligibility: h.eligibility, Definitions: h.definitions, Cache: h.cache,
	}
	return h
}

func csdReviewStage() dsl.StageDef {
	return dsl.StageDef{Type: "userTask", NodeID: "review", Role: "approver"}
}

func csdOtherStage() dsl.StageDef {
	return dsl.StageDef{Type: "userTask", NodeID: "audit", Role: "auditor"}
}

func (h *csdTaskHarness) requireTaskUntouched(t *testing.T) {
	t.Helper()
	require.EqualValues(t, csdTaskVersion, h.task.RecordVersion, "the task's record version must not move")
	require.Empty(t, h.assignments.setLeads, "no lead may be set")
	require.Equal(t, uuid.Nil, h.assignments.leadOf(h.task.ID), "the task must still have no lead")
}

// csdSignalPayloadTemporal records the payload of every signal it is sent.
type csdPayloadSignal struct {
	csdSignalCall
	Payload any
}

type csdPayloadTemporal struct {
	port.TemporalClient
	signals []csdPayloadSignal
}

func (c *csdPayloadTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	c.signals = append(c.signals, csdPayloadSignal{csdSignalCall{temporalWorkflowID, instanceID, signalName}, payload})
	return nil
}
