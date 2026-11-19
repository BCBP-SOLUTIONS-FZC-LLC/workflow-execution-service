package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// The rig serves the real task service behind the real handler. Only the outer edges are fakes (repositories,
// Definition Service, eligibility read, workflow client); each records the tenant and ids it receives.

const (
	w26aTaskVersion    = 3
	w26aUpstreamType   = "https://errors.bcbp.io/execution/upstream-unavailable"
	w26aInternalDetail = "an unexpected error occurred"
)

type w26aLookup struct{ tenant, id uuid.UUID }

type w26aProblem struct {
	Type   string `json:"type"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
	Code   string `json:"code"`
}

func w26aProblemOf(t *testing.T, rec *httptest.ResponseRecorder, status int) w26aProblem {
	t.Helper()
	require.Equal(t, status, rec.Code, rec.Body.String())
	var p w26aProblem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p), rec.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

type w26aTasks struct {
	port.TaskRepository
	rows   []*domain.Task
	getErr error
	// failReadsAfter, when above zero, makes every read after that many reads fail with getErr.
	failReadsAfter int
	reads          []w26aLookup
	lists          []w26aLookup
}

// GetByID answers a copy of the stored row, as a database read does.
func (r *w26aTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w26aLookup{tenantID, id})
	if r.getErr != nil && (r.failReadsAfter == 0 || len(r.reads) > r.failReadsAfter) {
		return nil, r.getErr
	}
	for _, t := range r.rows {
		if t.ID == id && t.TenantID == tenantID {
			snapshot := *t
			return &snapshot, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *w26aTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, w26aLookup{tenantID, instanceID})
	var out []*domain.Task
	for _, t := range r.rows {
		if t.TenantID == tenantID && t.WorkflowInstanceID == instanceID {
			out = append(out, t)
		}
	}
	return out, nil, nil
}

type w26aClaim struct {
	tenant, task, assignment uuid.UUID
	recordVersion            int64
}

// w26aAssignments lists a task's active assignments; ClaimLead plays the repository's part for the claimed
// assignment (it becomes lead and the task version moves by one).
type w26aAssignments struct {
	port.TaskAssignmentRepository
	tasks    *w26aTasks
	rows     []*domain.TaskAssignment
	listErr  error
	lists    []w26aLookup
	claims   []w26aClaim
	claimErr error
}

func (r *w26aAssignments) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lists = append(r.lists, w26aLookup{tenantID, taskID})
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []*domain.TaskAssignment
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *w26aAssignments) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	out := map[uuid.UUID]int{}
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.IsActive {
			out[a.TaskID]++
		}
	}
	return out, nil
}

func (r *w26aAssignments) ClaimLead(_ context.Context, tenantID, taskID, id uuid.UUID, recordVersion int64) (*domain.TaskAssignment, error) {
	r.claims = append(r.claims, w26aClaim{tenantID, taskID, id, recordVersion})
	if r.claimErr != nil {
		return nil, r.claimErr
	}
	var claimed *domain.TaskAssignment
	for _, a := range r.rows {
		if a.ID == id {
			a.IsLead = true
			claimed = a
		}
	}
	for _, task := range r.tasks.rows {
		if task.ID == taskID {
			task.RecordVersion++
		}
	}
	return claimed, nil
}

type w26aInstances struct {
	port.InstanceRepository
	rows   []*domain.Instance
	getErr error
	reads  []w26aLookup
}

func (r *w26aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w26aLookup{tenantID, id})
	if r.getErr != nil {
		return nil, r.getErr
	}
	for _, i := range r.rows {
		if i.ID == id && i.TenantID == tenantID {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w26aWorkflowCall struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    any
}

type w26aTemporal struct {
	port.TemporalClient
	signalErr error
	updateErr error
	onUpdate  func()
	signals   []w26aWorkflowCall
	updates   []w26aWorkflowCall
}

func (c *w26aTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w26aWorkflowCall{workflowID, instanceID, name, payload})
	return c.signalErr
}

func (c *w26aTemporal) UpdateWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.updates = append(c.updates, w26aWorkflowCall{workflowID, instanceID, name, payload})
	if c.updateErr != nil {
		return c.updateErr
	}
	if c.onUpdate != nil {
		c.onUpdate()
	}
	return nil
}

type w26aEligibilityCall struct {
	tenant, user, department uuid.UUID
	level                    string
}

type w26aEligibility struct {
	port.EligibilityChecker
	eligible bool
	err      error
	calls    []w26aEligibilityCall
}

func (e *w26aEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.calls = append(e.calls, w26aEligibilityCall{tenantID, userID, departmentID, level})
	return e.eligible && e.err == nil, e.err
}

type w26aDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []w26aLookup
}

func (d *w26aDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w26aLookup{tenantID, versionID})
	return d.resp, d.err
}

// w26aRig is the caller's tenant with one running instance and one ready task of the department "finance"
// at the node finance/review, plus every collaborator of the task service.
type w26aRig struct {
	tasks       *w26aTasks
	assignments *w26aAssignments
	instances   *w26aInstances
	temporal    *w26aTemporal
	eligibility *w26aEligibility
	definitions *w26aDefinitions

	withPlanCheck bool
	deptID        uuid.UUID
	versionID     uuid.UUID
	instance      *domain.Instance
	task          *domain.Task
}

func w26aNewRig(t *testing.T, assigneeMode string) *w26aRig {
	t.Helper()
	rig := &w26aRig{deptID: uuid.New(), versionID: uuid.New()}
	rig.instance = &domain.Instance{
		ID: testInstID, TenantID: testTenantID, WorkflowVersionID: rig.versionID,
		TemporalWorkflowID: "tenant:subject:key", Status: domain.InstanceStatusRunning,
	}
	rig.task = &domain.Task{
		ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, DepartmentID: rig.deptID,
		NodeKey: "finance/review", Status: domain.TaskStatusReady, AssigneeMode: assigneeMode, RecordVersion: w26aTaskVersion,
	}
	rig.tasks = &w26aTasks{rows: []*domain.Task{rig.task}}
	rig.assignments = &w26aAssignments{tasks: rig.tasks}
	rig.instances = &w26aInstances{rows: []*domain.Instance{rig.instance}}
	rig.temporal = &w26aTemporal{}
	rig.eligibility = &w26aEligibility{eligible: true}
	plan := w26aPlanJSON(t, rig.deptID)
	rig.definitions = &w26aDefinitions{resp: &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: rig.versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: plan,
	}}
	return rig
}

func w26aPlanJSON(t *testing.T, deptID uuid.UUID) string {
	t.Helper()
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{
				ID: "finance", IAMDepartmentID: deptID.String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "approver"}},
			}},
		}},
	})
	require.NoError(t, err)
	return string(raw)
}

// assign adds an active assignment of the task for user and returns it.
func (r *w26aRig) assign(user uuid.UUID) *domain.TaskAssignment {
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: testTenantID, TaskID: testTaskID, UserID: user, IsActive: true}
	r.assignments.rows = append(r.assignments.rows, a)
	return a
}

// addCompletedStep adds a finished task to the instance, the step a defer goes back to.
func (r *w26aRig) addCompletedStep() {
	r.tasks.rows = append(r.tasks.rows, &domain.Task{
		ID: uuid.New(), TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/intake",
		Status: domain.TaskStatusCompleted, AssigneeMode: "single", RecordVersion: 1,
	})
}

func (r *w26aRig) router() *gin.Engine {
	svc := &service.TaskService{
		Tasks: r.tasks, Assignments: r.assignments, Instances: r.instances, Temporal: r.temporal,
	}
	if r.withPlanCheck {
		svc.Definitions = r.definitions
		svc.Eligibility = r.eligibility
	} else {
		fx10AllowEligibility(svc, r.task.DepartmentID)
	}
	return newRouter(handler.New(handler.Services{Tasks: svc}))
}

func (r *w26aRig) taskPath(op string) string {
	if op == "" {
		return "/api/v1/tasks/" + r.task.ID.String()
	}
	return hreTaskPath(r.task.ID.String(), op)
}

func (r *w26aRig) post(op string, body map[string]any, admin bool) *httptest.ResponseRecorder {
	httpReq := req(http.MethodPost, r.taskPath(op), body)
	if admin {
		httpReq = asAdmin(httpReq)
	}
	return do(r.router(), httpReq)
}

func (r *w26aRig) requireNoWorkflowEffect(t *testing.T) {
	t.Helper()
	assert.Empty(t, r.temporal.signals, "no signal may reach the workflow")
	assert.Empty(t, r.temporal.updates, "no update may reach the workflow")
}

func (r *w26aRig) requireTaskUntouched(t *testing.T) {
	t.Helper()
	assert.Empty(t, r.assignments.claims, "no lead may be set")
	assert.EqualValues(t, w26aTaskVersion, r.task.RecordVersion, "the task version must not move")
}

func w26aDecodePayload(t *testing.T, payload any, into any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, into))
}
