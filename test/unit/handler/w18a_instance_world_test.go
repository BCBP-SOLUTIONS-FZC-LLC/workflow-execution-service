package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// Each error carries text that must never reach a response body.
var (
	errW18aDatabase   = errors.New("w18a database password=hunter2 refused")
	errW18aTemporal   = errors.New("w18a temporal frontend 10.9.8.7:7233 refused")
	errW18aOutbox     = errors.New("w18a outbox table locked")
	errW18aTenantRead = errors.New("w18a tenant state read failed")
)

type w18aRead struct{ tenantID, id uuid.UUID }

type w18aLookup struct {
	tenantID          uuid.UUID
	subjectID, bizKey string
}

type w18aRunID struct {
	tenantID, id uuid.UUID
	runID        string
}

type w18aList struct {
	tenantID uuid.UUID
	filter   port.InstanceListFilter
	page     port.PageRequest
}

// w18aInstances keeps committed rows apart from rows written inside a transaction, so a rolled-back
// transaction leaves nothing behind, and records every call with the tenant it carried.
type w18aInstances struct {
	port.InstanceRepository
	mu        sync.Mutex
	committed map[uuid.UUID]*domain.Instance
	staged    []*domain.Instance
	getErr    error
	liveErr   error
	listErr   error
	listRows  []*domain.Instance
	reads     []w18aRead
	lookups   []w18aLookup
	creates   []*domain.Instance
	runIDs    []w18aRunID
	lists     []w18aList
}

func w18aNewInstances() *w18aInstances {
	return &w18aInstances{committed: map[uuid.UUID]*domain.Instance{}}
}

func (r *w18aInstances) seed(inst *domain.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed[inst.ID] = inst
}

func (r *w18aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, w18aRead{tenantID, id})
	if r.getErr != nil {
		return nil, r.getErr
	}
	inst, ok := r.committed[id]
	if !ok || inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return inst, nil
}

func (r *w18aInstances) GetLiveByBusinessKey(_ context.Context, tenantID uuid.UUID, subjectID, businessKey string) (*domain.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lookups = append(r.lookups, w18aLookup{tenantID, subjectID, businessKey})
	if r.liveErr != nil {
		return nil, r.liveErr
	}
	for _, inst := range r.committed {
		if inst.TenantID == tenantID && inst.SubjectID == subjectID && inst.BusinessKey == businessKey {
			return inst, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *w18aInstances) Create(_ context.Context, inst *domain.Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	stored := *inst
	r.creates = append(r.creates, &stored)
	for _, live := range r.committed {
		if live.TenantID == inst.TenantID && live.SubjectID == inst.SubjectID && live.BusinessKey == inst.BusinessKey {
			return domain.ErrDuplicateBusinessKey
		}
	}
	r.staged = append(r.staged, &stored)
	return nil
}

func (r *w18aInstances) RecordRunID(_ context.Context, tenantID, id uuid.UUID, runID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runIDs = append(r.runIDs, w18aRunID{tenantID, id, runID})
	if inst, ok := r.committed[id]; ok && inst.TenantID == tenantID {
		inst.TemporalRunID = runID
	}
	return nil
}

func (r *w18aInstances) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, page port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lists = append(r.lists, w18aList{tenantID, filter, page})
	return r.listRows, nil, r.listErr
}

func (r *w18aInstances) committedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.committed)
}

// w18aTransactor commits the rows written inside the callback only when it returns nil.
type w18aTransactor struct{ instances *w18aInstances }

func (t w18aTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	t.instances.mu.Lock()
	t.instances.staged = nil
	t.instances.mu.Unlock()
	err := fn(ctx)
	t.instances.mu.Lock()
	defer t.instances.mu.Unlock()
	if err == nil {
		for _, inst := range t.instances.staged {
			t.instances.committed[inst.ID] = inst
		}
	}
	t.instances.staged = nil
	return err
}

func (t w18aTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

type w18aOutbox struct {
	port.OutboxRepository
	mu       sync.Mutex
	err      error
	enqueued []events.Envelope[json.RawMessage]
}

func (o *w18aOutbox) Enqueue(_ context.Context, env events.Envelope[json.RawMessage]) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	o.enqueued = append(o.enqueued, env)
	return nil
}

type w18aSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    []byte
}

type w18aTemporalClient struct {
	port.TemporalClient
	mu        sync.Mutex
	waiting   []string
	queryErr  error
	signalErr error
	startFunc func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error)
	queries   []string
	signals   []w18aSignal
	starts    []port.StartWorkflowInput
}

func (c *w18aTemporalClient) QueryWorkflow(_ context.Context, workflowID, queryType string, out any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, workflowID+" "+queryType)
	if c.queryErr != nil {
		return c.queryErr
	}
	raw, err := json.Marshal(c.waiting)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (c *w18aTemporalClient) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, w18aSignal{workflowID, instanceID, name, raw})
	return c.signalErr
}

func (c *w18aTemporalClient) StartWorkflow(ctx context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	c.mu.Lock()
	c.starts = append(c.starts, in)
	fn := c.startFunc
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "w18a-run"}, nil
}

type w18aDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []w18aRead
}

func (d *w18aDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w18aRead{tenantID, versionID})
	return d.resp, d.err
}

type w18aEligibilityCall struct {
	tenantID uuid.UUID
	requests []port.EligibilityCheckRequest
}

type w18aEligibility struct {
	port.EligibilityChecker
	calls []w18aEligibilityCall
}

func (e *w18aEligibility) CheckEligibilityBatch(_ context.Context, tenantID uuid.UUID, requests []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	e.calls = append(e.calls, w18aEligibilityCall{tenantID, requests})
	results := make([]port.EligibilityResult, len(requests))
	for i := range results {
		results[i] = port.EligibilityResult{Eligible: true}
	}
	return results, nil
}

type w18aRegistered struct {
	tenantID  uuid.UUID
	queueName string
}

type w18aQueues struct {
	port.ActiveTaskQueueRepository
	getErr      error
	registerErr error
	lookups     []string
	registered  []w18aRegistered
}

func (q *w18aQueues) GetByQueueName(_ context.Context, name string) (*domain.ActiveTaskQueue, error) {
	q.lookups = append(q.lookups, name)
	if q.getErr != nil {
		return nil, q.getErr
	}
	return nil, domain.ErrNotFound
}

func (q *w18aQueues) Register(_ context.Context, tenantID uuid.UUID, name string) (*domain.ActiveTaskQueue, error) {
	q.registered = append(q.registered, w18aRegistered{tenantID, name})
	if q.registerErr != nil {
		return nil, q.registerErr
	}
	return &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}, nil
}

type w18aGrants struct {
	granted bool
	asked   []uuid.UUID
}

func (g *w18aGrants) HasFeature(_ context.Context, tenantID uuid.UUID, _ string) (bool, error) {
	g.asked = append(g.asked, tenantID)
	return g.granted, nil
}

type w18aTenantStates struct {
	port.TenantStateRepository
	err   error
	reads []uuid.UUID
}

func (s *w18aTenantStates) Get(_ context.Context, tenantID uuid.UUID) (string, bool, error) {
	s.reads = append(s.reads, tenantID)
	return "", false, s.err
}

type w18aValidator struct{ types []string }

func (v *w18aValidator) Validate(_ context.Context, eventType string, _ json.RawMessage) error {
	v.types = append(v.types, eventType)
	return nil
}

// w18aWorld serves the real instance service behind the real routes. A test sets the knobs it needs
// before the first request.
type w18aWorld struct {
	instances   *w18aInstances
	outbox      *w18aOutbox
	temporal    *w18aTemporalClient
	definitions *w18aDefinitions
	eligibility *w18aEligibility
	queues      *w18aQueues
	states      *w18aTenantStates
	validator   *w18aValidator
	svc         *service.InstanceService
	router      *gin.Engine
	versionID   uuid.UUID
}

func w18aPlanJSON(t *testing.T, stages ...dsl.StageDef) string {
	t.Helper()
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		MainPlan: "main", SchemaVersion: dsl.CurrentSchemaVersion,
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", IAMDepartmentID: uuid.NewString(), Stages: stages}},
		}},
	})
	require.NoError(t, err)
	return string(raw)
}

func w18aStage(node string, assignees ...uuid.UUID) dsl.StageDef {
	ids := make([]string, len(assignees))
	for i, id := range assignees {
		ids[i] = id.String()
	}
	return dsl.StageDef{Type: "userTask", NodeID: node, DefaultAssignees: ids}
}

func w18aOverBudgetPlanJSON(t *testing.T) string {
	t.Helper()
	stages := make([]dsl.StageDef, domain.MaxPlanStages+1)
	for i := range stages {
		stages[i] = dsl.StageDef{Type: "approve", NodeID: fmt.Sprintf("T%d", i)}
	}
	return w18aPlanJSON(t, stages...)
}

func w18aNewWorld(t *testing.T, planJSON string) *w18aWorld {
	t.Helper()
	versionID := uuid.New()
	w := &w18aWorld{
		instances:   w18aNewInstances(),
		outbox:      &w18aOutbox{},
		temporal:    &w18aTemporalClient{},
		definitions: &w18aDefinitions{},
		eligibility: &w18aEligibility{},
		queues:      &w18aQueues{},
		states:      &w18aTenantStates{},
		validator:   &w18aValidator{},
		versionID:   versionID,
	}
	w.definitions.resp = &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: planJSON,
	}
	w.svc = &service.InstanceService{
		Instances:   w.instances,
		Outbox:      w.outbox,
		Transactor:  w18aTransactor{instances: w.instances},
		Temporal:    w.temporal,
		Definitions: w.definitions,
		Eligibility: w.eligibility,
		Queues:      w.queues,
		Validator:   w.validator,
	}
	w.router = newRouter(handler.New(handler.Services{Instances: w.svc}))
	return w
}

// seedInstance stores a running instance of the caller's tenant, waiting at the fixed test id.
func (w *w18aWorld) seedInstance(status domain.InstanceStatus) *domain.Instance {
	inst := &domain.Instance{
		ID: testInstID, TenantID: testTenantID, WorkflowVersionID: w.versionID,
		TemporalWorkflowID: "tenant:subject:w18a", Status: status, RecordVersion: 1,
	}
	w.instances.seed(inst)
	return inst
}
