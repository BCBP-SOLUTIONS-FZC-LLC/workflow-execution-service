package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errCscDatabase = errors.New("csc: database unavailable")
	errCscOutbox   = errors.New("csc: outbox write failed")
)

// cscTx runs the callback directly and records what the transaction saw: the
// tenant carried on the context it was given, whether it is still open, and
// how the callback ended (an error from the callback is what makes a real
// transaction roll back).
type cscTx struct {
	active    bool
	runs      int
	guTenants []string
	lastErr   error
}

func (t *cscTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return t.run(ctx, fn)
}

func (t *cscTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.run(ctx, fn)
}

func (t *cscTx) run(ctx context.Context, fn func(context.Context) error) error {
	t.runs++
	if guc, ok := pgcommon.GUCSetFromContext(ctx); ok {
		t.guTenants = append(t.guTenants, guc.TenantID)
	} else {
		t.guTenants = append(t.guTenants, "")
	}
	t.active = true
	err := fn(ctx)
	t.active = false
	t.lastErr = err
	return err
}

type cscGetCall struct {
	TenantID   uuid.UUID
	InstanceID uuid.UUID
	InTx       bool
}

type cscUpdateCall struct {
	TenantID      uuid.UUID
	InstanceID    uuid.UUID
	Status        domain.InstanceStatus
	RecordVersion int64
	InTx          bool
}

// cscInstanceRepo answers only for the tenant that owns the row, the way
// row-level security does, and records every call with its arguments.
type cscInstanceRepo struct {
	port.InstanceRepository
	tx     *cscTx
	byID   map[uuid.UUID]*domain.Instance
	getErr error
	// getErrOnCall fails the Nth GetByID call (1-based) with the given error.
	getErrOnCall map[int]error
	gets         []cscGetCall
	updates      []cscUpdateCall
}

func newCscInstanceRepo(tx *cscTx, instances ...*domain.Instance) *cscInstanceRepo {
	r := &cscInstanceRepo{tx: tx, byID: map[uuid.UUID]*domain.Instance{}}
	for _, i := range instances {
		r.byID[i.ID] = i
	}
	return r
}

func (r *cscInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gets = append(r.gets, cscGetCall{TenantID: tenantID, InstanceID: id, InTx: r.tx.active})
	if err := r.getErrOnCall[len(r.gets)]; err != nil {
		return nil, err
	}
	if r.getErr != nil {
		return nil, r.getErr
	}
	inst, ok := r.byID[id]
	if !ok || inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	copied := *inst
	return &copied, nil
}

func (r *cscInstanceRepo) UpdateStatus(_ context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, recordVersion int64) (*domain.Instance, error) {
	r.updates = append(r.updates, cscUpdateCall{TenantID: tenantID, InstanceID: id, Status: status, RecordVersion: recordVersion, InTx: r.tx.active})
	inst, ok := r.byID[id]
	if !ok || inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.Status = status
	inst.RecordVersion++
	copied := *inst
	return &copied, nil
}

type cscTaskUpdate struct {
	TenantID      uuid.UUID
	TaskID        uuid.UUID
	Status        domain.TaskStatus
	RecordVersion int64
	InTx          bool
}

type cscTaskListCall struct {
	TenantID   uuid.UUID
	InstanceID uuid.UUID
}

type cscTaskRepo struct {
	port.TaskRepository
	tx      *cscTx
	byID    map[uuid.UUID]*domain.Task
	lists   []cscTaskListCall
	updates []cscTaskUpdate
}

func newCscTaskRepo(tx *cscTx, tasks ...*domain.Task) *cscTaskRepo {
	r := &cscTaskRepo{tx: tx, byID: map[uuid.UUID]*domain.Task{}}
	for _, t := range tasks {
		r.byID[t.ID] = t
	}
	return r
}

func (r *cscTaskRepo) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, cscTaskListCall{TenantID: tenantID, InstanceID: instanceID})
	var out []*domain.Task
	for _, t := range r.byID {
		if t.TenantID == tenantID && t.WorkflowInstanceID == instanceID {
			copied := *t
			out = append(out, &copied)
		}
	}
	return out, nil, nil
}

func (r *cscTaskRepo) UpdateStatus(_ context.Context, tenantID, id uuid.UUID, status domain.TaskStatus, recordVersion int64) (*domain.Task, error) {
	r.updates = append(r.updates, cscTaskUpdate{TenantID: tenantID, TaskID: id, Status: status, RecordVersion: recordVersion, InTx: r.tx.active})
	t, ok := r.byID[id]
	if !ok || t.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if t.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	t.Status = status
	t.RecordVersion++
	copied := *t
	return &copied, nil
}

type cscVacateCall struct {
	TenantID     uuid.UUID
	AssignmentID uuid.UUID
}

type cscAssignmentRepo struct {
	port.TaskAssignmentRepository
	byID    map[uuid.UUID]*domain.TaskAssignment
	vacated []cscVacateCall
}

func newCscAssignmentRepo(assignments ...*domain.TaskAssignment) *cscAssignmentRepo {
	r := &cscAssignmentRepo{byID: map[uuid.UUID]*domain.TaskAssignment{}}
	for _, a := range assignments {
		r.byID[a.ID] = a
	}
	return r
}

func (r *cscAssignmentRepo) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.TenantID == tenantID && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *cscAssignmentRepo) Vacate(_ context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.vacated = append(r.vacated, cscVacateCall{TenantID: tenantID, AssignmentID: id})
	a, ok := r.byID[id]
	if !ok || a.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	a.IsActive = false
	return a, nil
}

type cscEnqueued struct {
	Envelope events.Envelope[json.RawMessage]
	InTx     bool
}

type cscOutbox struct {
	port.OutboxRepository
	tx         *cscTx
	enqueueErr error
	attempts   []cscEnqueued
}

func (o *cscOutbox) Enqueue(_ context.Context, env events.Envelope[json.RawMessage]) error {
	o.attempts = append(o.attempts, cscEnqueued{Envelope: env, InTx: o.tx.active})
	return o.enqueueErr
}

type cscSignal struct {
	WorkflowID string
	InstanceID uuid.UUID
	Name       string
	Payload    any
}

type cscTerminate struct {
	WorkflowID string
	RunID      string
	Reason     string
}

type cscTemporal struct {
	port.TemporalClient
	signalErr  error
	signals    []cscSignal
	terminates []cscTerminate
}

func (c *cscTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, cscSignal{WorkflowID: workflowID, InstanceID: instanceID, Name: name, Payload: payload})
	return c.signalErr
}

func (c *cscTemporal) TerminateWorkflow(_ context.Context, workflowID, runID, reason string) error {
	c.terminates = append(c.terminates, cscTerminate{WorkflowID: workflowID, RunID: runID, Reason: reason})
	return nil
}

type cscPlanRead struct {
	TenantID  uuid.UUID
	VersionID uuid.UUID
}

type cscDefinitions struct {
	port.DefinitionServiceClient
	resp  *port.CompiledWorkflow
	reads []cscPlanRead
}

func (d *cscDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, cscPlanRead{TenantID: tenantID, VersionID: versionID})
	return d.resp, nil
}

type cscHarness struct {
	svc         *service.InstanceService
	tx          *cscTx
	instances   *cscInstanceRepo
	tasks       *cscTaskRepo
	assignments *cscAssignmentRepo
	outbox      *cscOutbox
	temporal    *cscTemporal
	definitions *cscDefinitions
}

func newCscHarness(instances ...*domain.Instance) *cscHarness {
	tx := &cscTx{}
	h := &cscHarness{
		tx:          tx,
		instances:   newCscInstanceRepo(tx, instances...),
		tasks:       newCscTaskRepo(tx),
		assignments: newCscAssignmentRepo(),
		outbox:      &cscOutbox{tx: tx},
		temporal:    &cscTemporal{},
		definitions: &cscDefinitions{},
	}
	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		panic(err)
	}
	h.svc = &service.InstanceService{
		Instances:   h.instances,
		Tasks:       h.tasks,
		Assignments: h.assignments,
		Outbox:      h.outbox,
		Transactor:  tx,
		Temporal:    h.temporal,
		Definitions: h.definitions,
		Validator:   validator,
	}
	return h
}

func newCscInstance(tenantID uuid.UUID, status domain.InstanceStatus, recordVersion int64) *domain.Instance {
	return &domain.Instance{
		ID:                 uuid.New(),
		TenantID:           tenantID,
		WorkflowVersionID:  uuid.New(),
		SubjectID:          "SUBJ-1",
		BusinessKey:        "BK-1",
		TemporalWorkflowID: tenantID.String() + ":SUBJ-1:BK-1",
		TemporalRunID:      "run-" + uuid.NewString(),
		Status:             status,
		RecordVersion:      recordVersion,
		StartedByUserID:    uuid.New(),
	}
}

func cscPlanJSON(t *testing.T, deptID string, nodeIDs ...string) string {
	t.Helper()
	stages := make([]dsl.StageDef, len(nodeIDs))
	for i, n := range nodeIDs {
		stages[i] = dsl.StageDef{Type: "userTask", NodeID: n}
	}
	collab := dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			TaskQueue:   "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: deptID, Label: deptID, Stages: stages}},
		}},
	}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

// cscWire is the shape of the admin signal payload the service hands to the
// Temporal client, read back through JSON.
type cscWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetNodeKey string
	RecordVersion int64
}

func cscDecodeWire(t *testing.T, payload any) cscWire {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	var w cscWire
	require.NoError(t, json.Unmarshal(b, &w))
	return w
}
