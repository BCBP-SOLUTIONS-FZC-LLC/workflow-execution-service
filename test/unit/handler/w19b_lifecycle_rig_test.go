package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// The rig runs the real instance service behind the real handler and the real Temporal client adapter; only the
// outer edges are fakes (repositories, Definition Service, cache, the Temporal SDK client). Every fake records
// the tenant and ids it receives.

const w19bTargetNode = "finance/Receive_Bid"

type w19bIDs struct{ tenantID, id uuid.UUID }

type w19bStatusWrite struct {
	tenantID, id  uuid.UUID
	status        domain.InstanceStatus
	recordVersion int64
}

type w19bInstanceRepo struct {
	port.InstanceRepository
	rows   map[uuid.UUID]*domain.Instance
	getErr error
	gets   []w19bIDs
	writes []w19bStatusWrite
}

func (r *w19bInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gets = append(r.gets, w19bIDs{tenantID, id})
	if r.getErr != nil {
		return nil, r.getErr
	}
	row, ok := r.rows[id]
	if !ok || row.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	copied := *row
	return &copied, nil
}

func (r *w19bInstanceRepo) UpdateStatus(_ context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, recordVersion int64) (*domain.Instance, error) {
	r.writes = append(r.writes, w19bStatusWrite{tenantID, id, status, recordVersion})
	row, ok := r.rows[id]
	if !ok || row.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if row.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	row.Status = status
	row.RecordVersion++
	copied := *row
	return &copied, nil
}

type w19bTaskList struct {
	tenantID, instanceID uuid.UUID
	limit                int
	after                *port.Cursor
}

type w19bTaskWrite struct {
	tenantID, id  uuid.UUID
	status        domain.TaskStatus
	recordVersion int64
}

// w19bTaskRepo pages like the real repository: the cursor names the last row of the previous page.
type w19bTaskRepo struct {
	port.TaskRepository
	rows         []*domain.Task
	failOnList   int
	listErr      error
	lists        []w19bTaskList
	statusWrites []w19bTaskWrite
}

func (r *w19bTaskRepo) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, w19bTaskList{tenantID, instanceID, page.Limit, page.After})
	if r.failOnList != 0 && len(r.lists) == r.failOnList {
		return nil, nil, r.listErr
	}
	var mine []*domain.Task
	for _, t := range r.rows {
		if t.TenantID == tenantID && t.WorkflowInstanceID == instanceID {
			mine = append(mine, t)
		}
	}
	start := 0
	if page.After != nil {
		for i, t := range mine {
			if t.ID == page.After.ID {
				start = i + 1
			}
		}
	}
	end := min(start+page.Limit, len(mine))
	out := mine[start:end]
	if end < len(mine) {
		return out, &port.Cursor{ID: out[len(out)-1].ID}, nil
	}
	return out, nil, nil
}

func (r *w19bTaskRepo) UpdateStatus(_ context.Context, tenantID, id uuid.UUID, status domain.TaskStatus, recordVersion int64) (*domain.Task, error) {
	r.statusWrites = append(r.statusWrites, w19bTaskWrite{tenantID, id, status, recordVersion})
	for _, t := range r.rows {
		if t.TenantID == tenantID && t.ID == id {
			t.Status = status
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w19bAssignments struct {
	port.TaskAssignmentRepository
	lookups []w19bIDs
}

func (a *w19bAssignments) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	a.lookups = append(a.lookups, w19bIDs{tenantID, taskID})
	return nil, nil
}

type w19bEvent struct{ eventType, tenantID, subject string }

type w19bOutbox struct {
	port.OutboxRepository
	types  []string
	events []w19bEvent
}

func (o *w19bOutbox) Enqueue(_ context.Context, env events.Envelope[json.RawMessage]) error {
	o.types = append(o.types, env.Type)
	o.events = append(o.events, w19bEvent{env.Type, env.TenantID, env.Subject})
	return nil
}

type w19bAcceptingValidator struct{}

func (w19bAcceptingValidator) Validate(context.Context, string, json.RawMessage) error { return nil }

type w19bTransactor struct {
	port.Transactor
	retried int
	plain   int
}

func (x *w19bTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	x.plain++
	return fn(ctx)
}

func (x *w19bTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	x.retried++
	return fn(ctx)
}

type w19bTerminateCall struct{ workflowID, runID, reason string }

type w19bSignalCall struct {
	workflowID, runID, channel string
	payload                    any
}

// w19bSDK stands in for the Temporal SDK client under the real adapter.
type w19bSDK struct {
	client.Client
	terminateErr func(call int) error
	signalErr    error
	onTerminate  func(call int)
	terminates   []w19bTerminateCall
	signals      []w19bSignalCall
}

func (s *w19bSDK) TerminateWorkflow(_ context.Context, workflowID, runID, reason string, _ ...interface{}) error {
	s.terminates = append(s.terminates, w19bTerminateCall{workflowID, runID, reason})
	call := len(s.terminates)
	if s.onTerminate != nil {
		s.onTerminate(call)
	}
	if s.terminateErr != nil {
		return s.terminateErr(call)
	}
	return nil
}

func (s *w19bSDK) SignalWorkflow(_ context.Context, workflowID, runID, channel string, payload interface{}) error {
	s.signals = append(s.signals, w19bSignalCall{workflowID, runID, channel, payload})
	return s.signalErr
}

type w19bPlanRead struct{ tenantID, versionID uuid.UUID }

type w19bDefinitions struct {
	planJSON string
	err      error
	reads    []w19bPlanRead
}

func (d *w19bDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, w19bPlanRead{tenantID, versionID})
	if d.err != nil {
		return nil, d.err
	}
	return &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: d.planJSON,
	}, nil
}

type w19bCacheSet struct {
	key, value string
	ttl        time.Duration
}

type w19bCache struct {
	port.CacheStore
	entries map[string]string
	getErr  error
	setErr  error
	gets    []string
	sets    []w19bCacheSet
}

func (c *w19bCache) Get(_ context.Context, key string) (string, error) {
	c.gets = append(c.gets, key)
	if c.getErr != nil {
		return "", c.getErr
	}
	return c.entries[key], nil
}

func (c *w19bCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.sets = append(c.sets, w19bCacheSet{key, value, ttl})
	if c.setErr != nil {
		return c.setErr
	}
	c.entries[key] = value
	return nil
}

type w19bLog struct{ warns int }

func (l *w19bLog) Debug(string, map[string]any) {}
func (l *w19bLog) Info(string, map[string]any)  {}
func (l *w19bLog) Warn(string, map[string]any)  { l.warns++ }
func (l *w19bLog) Error(string, map[string]any) {}

type w19bRig struct {
	instances   *w19bInstanceRepo
	tasks       *w19bTaskRepo
	assignments *w19bAssignments
	outbox      *w19bOutbox
	transactor  *w19bTransactor
	sdk         *w19bSDK
	defs        *w19bDefinitions
	cache       *w19bCache
	log         *w19bLog
	router      *gin.Engine
}

func w19bNewRig(t *testing.T) *w19bRig {
	t.Helper()
	rig := &w19bRig{
		instances:   &w19bInstanceRepo{rows: map[uuid.UUID]*domain.Instance{}},
		tasks:       &w19bTaskRepo{},
		assignments: &w19bAssignments{},
		outbox:      &w19bOutbox{},
		transactor:  &w19bTransactor{},
		sdk:         &w19bSDK{},
		defs:        &w19bDefinitions{planJSON: w19bPlanJSON(t)},
		cache:       &w19bCache{entries: map[string]string{}},
		log:         &w19bLog{},
	}
	svc := &service.InstanceService{
		Instances: rig.instances, Tasks: rig.tasks, Assignments: rig.assignments, Outbox: rig.outbox,
		Transactor: rig.transactor, Temporal: temporalclient.New(rig.sdk), Definitions: rig.defs,
		Validator: w19bAcceptingValidator{}, Cache: rig.cache, Log: rig.log,
	}
	rig.router = newRouter(handler.New(handler.Services{Instances: svc}))
	return rig
}

// seed stores an instance of the caller's tenant.
func (r *w19bRig) seed(status domain.InstanceStatus, recordVersion int64) *domain.Instance {
	id := uuid.New()
	inst := &domain.Instance{
		ID: id, TenantID: testTenantID, WorkflowVersionID: uuid.New(), Status: status,
		RecordVersion: recordVersion, TemporalWorkflowID: "wf-" + id.String(),
	}
	r.instances.rows[id] = inst
	return inst
}

func (r *w19bRig) addTask(inst *domain.Instance, status domain.TaskStatus) *domain.Task {
	task := &domain.Task{
		ID: uuid.New(), TenantID: inst.TenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/Receive_Bid",
		Status: status, RecordVersion: 1,
	}
	r.tasks.rows = append(r.tasks.rows, task)
	return task
}

func (r *w19bRig) post(path string, body any) *httptest.ResponseRecorder {
	return do(r.router, asAdmin(req(http.MethodPost, path, body)))
}

func (r *w19bRig) terminate(id uuid.UUID) *httptest.ResponseRecorder {
	return r.post(instancePath(id)+"/terminate", map[string]any{"reason": "stuck"})
}

func (r *w19bRig) forceForward(id uuid.UUID, recordVersion int64) *httptest.ResponseRecorder {
	return r.post(instancePath(id)+"/force-forward", map[string]any{"target_node_key": w19bTargetNode, "record_version": recordVersion})
}

func (r *w19bRig) forceBack(id uuid.UUID, recordVersion int64) *httptest.ResponseRecorder {
	return r.forceBackTo(id, "", recordVersion)
}

func (r *w19bRig) forceBackTo(id uuid.UUID, targetNodeKey string, recordVersion int64) *httptest.ResponseRecorder {
	return r.post(instancePath(id)+"/force-back", map[string]any{"target_node_key": targetNodeKey, "record_version": recordVersion})
}

func w19bCollabJSON(t *testing.T, collab dsl.CompiledCollaboration) string {
	t.Helper()
	collab.SchemaVersion = dsl.CurrentSchemaVersion
	raw, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(raw)
}

func w19bPlanJSON(t *testing.T) string {
	t.Helper()
	return w19bCollabJSON(t, dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", Stages: []dsl.StageDef{
				{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
			}}},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"finance"}}}},
		}},
	})
}

func w19bCachedPlan(t *testing.T, versionID uuid.UUID, planJSON string) string {
	t.Helper()
	raw, err := json.Marshal(port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: planJSON,
	})
	require.NoError(t, err)
	return string(raw)
}

func w19bPlanCacheKey(tenantID, versionID uuid.UUID) string {
	return "compiled_plan:" + tenantID.String() + ":" + versionID.String()
}

// w19bSignalBody is what the instance service puts on the wire for an admin signal.
type w19bSignalBody struct {
	AdminUserID   string
	TargetNodeKey string
	RecordVersion int64
}

func w19bDecodeSignal(t *testing.T, payload any) w19bSignalBody {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var body w19bSignalBody
	require.NoError(t, json.Unmarshal(raw, &body))
	return body
}

func w19bProblemOf(t *testing.T, w *httptest.ResponseRecorder) w13aProblem {
	t.Helper()
	var p w13aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	return p
}
