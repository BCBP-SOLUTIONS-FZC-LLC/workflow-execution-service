package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// cseLookup is one repository read: the tenant the caller scoped it to and
// the row it asked for.
type cseLookup struct {
	Tenant uuid.UUID
	ID     uuid.UUID
}

// cseTaskRepo is a tenant-scoped task store: a row of another tenant is as
// absent as an unknown id, the way row-level security makes it.
type cseTaskRepo struct {
	port.TaskRepository
	tasks      []*domain.Task
	getCalls   []cseLookup
	getErrs    []error // consumed one per GetByID call; nil means "look it up"
	listCalls  []cseLookup
	nodeCalls  []cseLookup
	nodeKeyArg []string
}

func (r *cseTaskRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.getCalls = append(r.getCalls, cseLookup{tenantID, id})
	if len(r.getErrs) > 0 {
		err := r.getErrs[0]
		r.getErrs = r.getErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	for _, t := range r.tasks {
		if t.ID == id && t.TenantID == tenantID {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *cseTaskRepo) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.listCalls = append(r.listCalls, cseLookup{tenantID, instanceID})
	var out []*domain.Task
	for _, t := range r.tasks {
		if t.WorkflowInstanceID == instanceID && t.TenantID == tenantID {
			out = append(out, t)
		}
	}
	return out, nil, nil
}

func (r *cseTaskRepo) GetByInstanceAndNode(_ context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*domain.Task, error) {
	r.nodeCalls = append(r.nodeCalls, cseLookup{tenantID, instanceID})
	r.nodeKeyArg = append(r.nodeKeyArg, nodeKey)
	for _, t := range r.tasks {
		if t.WorkflowInstanceID == instanceID && t.TenantID == tenantID && t.NodeKey == nodeKey {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

// cseAssignmentRepo keeps assignments in the order they were added.
type cseAssignmentRepo struct {
	port.TaskAssignmentRepository
	rows  []*domain.TaskAssignment
	calls []cseLookup
	err   error
}

func (r *cseAssignmentRepo) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.calls = append(r.calls, cseLookup{tenantID, taskID})
	if r.err != nil {
		return nil, r.err
	}
	var out []*domain.TaskAssignment
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

// cseInstanceRepo is a tenant-scoped instance store.
type cseInstanceRepo struct {
	port.InstanceRepository
	rows  []*domain.Instance
	calls []cseLookup
	err   error
}

func (r *cseInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.calls = append(r.calls, cseLookup{tenantID, id})
	if r.err != nil {
		return nil, r.err
	}
	for _, i := range r.rows {
		if i.ID == id && i.TenantID == tenantID {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

// cseTemporal records the workflow calls; the hooks let a test play the
// workflow's part.
type cseTemporal struct {
	port.TemporalClient
	signals  []signalCall
	updates  []signalCall
	queries  []string
	onUpdate func(instanceID uuid.UUID) error
}

func (c *cseTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	c.signals = append(c.signals, signalCall{TemporalWorkflowID: temporalWorkflowID, InstanceID: instanceID, SignalName: signalName, Payload: payload})
	return nil
}

func (c *cseTemporal) UpdateWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, updateName string, payload any) error {
	c.updates = append(c.updates, signalCall{TemporalWorkflowID: temporalWorkflowID, InstanceID: instanceID, SignalName: updateName, Payload: payload})
	if c.onUpdate != nil {
		return c.onUpdate(instanceID)
	}
	return nil
}

func (c *cseTemporal) QueryWorkflow(_ context.Context, _, queryType string, _ any) error {
	c.queries = append(c.queries, queryType)
	return nil
}

type cseEligibilityCall struct {
	Tenant, User, Department uuid.UUID
	Level                    string
}

type cseEligibility struct {
	port.EligibilityChecker
	calls    []cseEligibilityCall
	eligible bool
}

func (e *cseEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.calls = append(e.calls, cseEligibilityCall{tenantID, userID, departmentID, level})
	return e.eligible, nil
}

type cseDefinitions struct {
	port.DefinitionServiceClient
	resp  *port.CompiledWorkflow
	err   error
	calls []cseLookup
}

func (d *cseDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, cseLookup{tenantID, versionID})
	return d.resp, d.err
}

// cseTaskWorld is one tenant's instance with one human task on the node
// finance/review, assigned to one user, plus the services built over it.
type cseTaskWorld struct {
	svc         *service.TaskService
	tasks       *cseTaskRepo
	assignments *cseAssignmentRepo
	instances   *cseInstanceRepo
	temporal    *cseTemporal
	tenant      uuid.UUID
	instance    *domain.Instance
	task        *domain.Task
	user        uuid.UUID
}

func cseNewTaskWorld() *cseTaskWorld {
	w := &cseTaskWorld{tenant: uuid.New(), user: uuid.New()}
	w.instance = &domain.Instance{
		ID: uuid.New(), TenantID: w.tenant, WorkflowVersionID: uuid.New(),
		TemporalWorkflowID: "tenant:subject:key", Status: domain.InstanceStatusRunning,
	}
	w.task = &domain.Task{
		ID: uuid.New(), TenantID: w.tenant, WorkflowInstanceID: w.instance.ID, NodeKey: "finance/review",
		Status: domain.TaskStatusReady, AssigneeMode: "single", RecordVersion: 3,
	}
	w.tasks = &cseTaskRepo{tasks: []*domain.Task{w.task}}
	w.assignments = &cseAssignmentRepo{rows: []*domain.TaskAssignment{
		{ID: uuid.New(), TenantID: w.tenant, TaskID: w.task.ID, UserID: w.user, IsActive: true},
	}}
	w.instances = &cseInstanceRepo{rows: []*domain.Instance{w.instance}}
	w.temporal = &cseTemporal{}
	w.svc = &service.TaskService{
		Instances: w.instances, Tasks: w.tasks, Assignments: w.assignments, Temporal: w.temporal,
	}
	return w
}

// withCompletedStep adds a finished task to the instance, the step a defer
// goes back to.
func (w *cseTaskWorld) withCompletedStep() {
	w.tasks.tasks = append(w.tasks.tasks, &domain.Task{
		ID: uuid.New(), TenantID: w.tenant, WorkflowInstanceID: w.instance.ID, NodeKey: "finance/intake",
		Status: domain.TaskStatusCompleted, AssigneeMode: "single", RecordVersion: 1,
	})
}

// withEligibility configures the eligibility check over a plan holding the
// given stages of the department finance, and returns the two collaborators.
func (w *cseTaskWorld) withEligibility(t *testing.T, eligible bool, stages ...dsl.StageDef) (*cseEligibility, *cseDefinitions) {
	t.Helper()
	defs := &cseDefinitions{resp: publishedCompiledWorkflow(uuid.New(), w.instance.WorkflowVersionID, compiledPlanJSON(t, stages...))}
	elig := &cseEligibility{eligible: eligible}
	w.svc.Definitions = defs
	w.svc.Eligibility = elig
	return elig, defs
}

func cseRequireNoWorkflowCalls(t *testing.T, c *cseTemporal) {
	t.Helper()
	require.Empty(t, c.signals, "no signal may reach the workflow")
	require.Empty(t, c.updates, "no update may reach the workflow")
}
