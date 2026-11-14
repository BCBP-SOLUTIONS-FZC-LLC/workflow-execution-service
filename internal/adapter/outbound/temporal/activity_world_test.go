package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errWkactBoom = errors.New("wkact boom")

// wkactCall is one repository call as the fakes saw it: which operation, for
// which tenant, on which resource.
type wkactCall struct {
	Op     string
	Tenant uuid.UUID
	ID     uuid.UUID
	// Version is the record version the call was given; zero where the call takes none.
	Version int64
}

type wkactNodeWrite struct {
	Tenant  uuid.UUID
	ID      uuid.UUID
	Keys    []string
	Version int64
}

type wkactStatusWrite struct {
	Tenant  uuid.UUID
	ID      uuid.UUID
	Status  domain.InstanceStatus
	Version int64
}

type wkactSnapshot struct {
	instances   map[uuid.UUID]domain.Instance
	tasks       map[uuid.UUID]domain.Task
	assignments map[uuid.UUID]domain.TaskAssignment
	outbox      int
}

// wkactWorld is the in-memory store behind every fake of one test. Its
// transactor rolls the whole store back when the transaction callback returns
// an error, as the real transactor does.
type wkactWorld struct {
	instances    map[uuid.UUID]*domain.Instance
	tasks        map[uuid.UUID]*domain.Task
	assignments  map[uuid.UUID]*domain.TaskAssignment
	outbox       []events.Envelope[json.RawMessage]
	calls        []wkactCall
	nodeWrites   []wkactNodeWrite
	statusWrites []wkactStatusWrite
	failures     map[string]error
}

func wkactNewWorld() *wkactWorld {
	return &wkactWorld{
		instances:   map[uuid.UUID]*domain.Instance{},
		tasks:       map[uuid.UUID]*domain.Task{},
		assignments: map[uuid.UUID]*domain.TaskAssignment{},
		failures:    map[string]error{},
	}
}

func (w *wkactWorld) addInstance(tenant uuid.UUID, status domain.InstanceStatus) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenant, WorkflowVersionID: uuid.New(), SubjectID: "subject-" + uuid.NewString(),
		BusinessKey: "bk-" + uuid.NewString(), Status: status, StartedByUserID: uuid.New(), RecordVersion: 1,
	}
	w.instances[inst.ID] = inst
	return inst
}

func (w *wkactWorld) addTask(tenant uuid.UUID, inst *domain.Instance, nodeKey string, status domain.TaskStatus) *domain.Task {
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, NodeKey: nodeKey, DepartmentID: uuid.New(),
		Status: status, RecordVersion: 1,
	}
	w.tasks[task.ID] = task
	return task
}

func (w *wkactWorld) addAssignment(tenant uuid.UUID, task *domain.Task, active bool) *domain.TaskAssignment {
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenant, TaskID: task.ID, UserID: uuid.New(), IsActive: active}
	w.assignments[a.ID] = a
	return a
}

func (w *wkactWorld) fail(op string) error { return w.failures[op] }

func (w *wkactWorld) record(op string, tenant, id uuid.UUID) {
	w.calls = append(w.calls, wkactCall{Op: op, Tenant: tenant, ID: id})
}

func (w *wkactWorld) recordVersioned(op string, tenant, id uuid.UUID, version int64) {
	w.calls = append(w.calls, wkactCall{Op: op, Tenant: tenant, ID: id, Version: version})
}

func (w *wkactWorld) callsTo(op string) []wkactCall {
	var out []wkactCall
	for _, c := range w.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func (w *wkactWorld) requireAllCallsForTenant(t *testing.T, tenant uuid.UUID) {
	t.Helper()
	require.NotEmpty(t, w.calls)
	for _, c := range w.calls {
		require.Equal(t, tenant, c.Tenant, "%s ran for another tenant", c.Op)
	}
}

func (w *wkactWorld) eventsOfType(eventType string) []events.Envelope[json.RawMessage] {
	var out []events.Envelope[json.RawMessage]
	for _, env := range w.outbox {
		if env.Type == eventType {
			out = append(out, env)
		}
	}
	return out
}

func (w *wkactWorld) snapshot() wkactSnapshot {
	s := wkactSnapshot{
		instances:   map[uuid.UUID]domain.Instance{},
		tasks:       map[uuid.UUID]domain.Task{},
		assignments: map[uuid.UUID]domain.TaskAssignment{},
		outbox:      len(w.outbox),
	}
	for id, v := range w.instances {
		s.instances[id] = *v
	}
	for id, v := range w.tasks {
		s.tasks[id] = *v
	}
	for id, v := range w.assignments {
		s.assignments[id] = *v
	}
	return s
}

func (w *wkactWorld) restore(s wkactSnapshot) {
	for id, p := range w.instances {
		if v, ok := s.instances[id]; ok {
			*p = v
		} else {
			delete(w.instances, id)
		}
	}
	for id, p := range w.tasks {
		if v, ok := s.tasks[id]; ok {
			*p = v
		} else {
			delete(w.tasks, id)
		}
	}
	for id, p := range w.assignments {
		if v, ok := s.assignments[id]; ok {
			*p = v
		} else {
			delete(w.assignments, id)
		}
	}
	w.outbox = w.outbox[:s.outbox]
}

type wkactTxRecord struct {
	GUCSet bool
	Tenant string
}

// wkactTransactor records the tenant GUC found on the context handed to
// RunInTx. With skip set it does not run the callback.
type wkactTransactor struct {
	world      *wkactWorld
	skip       bool
	opened     []wkactTxRecord
	committed  int
	rolledBack int
}

func (x *wkactTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	g, ok := pgcommon.GUCSetFromContext(ctx)
	x.opened = append(x.opened, wkactTxRecord{GUCSet: ok, Tenant: g.TenantID})
	if x.skip {
		return nil
	}
	snap := x.world.snapshot()
	if err := fn(ctx); err != nil {
		x.world.restore(snap)
		x.rolledBack++
		return err
	}
	x.committed++
	return nil
}

func (x *wkactTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return x.RunInTx(ctx, fn)
}

type wkactInstances struct {
	port.InstanceRepository
	w *wkactWorld
}

func (r wkactInstances) GetByID(_ context.Context, tenant, id uuid.UUID) (*domain.Instance, error) {
	r.w.record("Instances.GetByID", tenant, id)
	if err := r.w.fail("Instances.GetByID"); err != nil {
		return nil, err
	}
	inst, ok := r.w.instances[id]
	if !ok || inst.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	return inst, nil
}

func (r wkactInstances) UpdateStatus(_ context.Context, tenant, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
	r.w.record("Instances.UpdateStatus", tenant, id)
	r.w.statusWrites = append(r.w.statusWrites, wkactStatusWrite{Tenant: tenant, ID: id, Status: status, Version: version})
	inst, ok := r.w.instances[id]
	if !ok || inst.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.Status = status
	inst.RecordVersion++
	return inst, nil
}

func (r wkactInstances) UpdateCurrentNodeKeys(_ context.Context, tenant, id uuid.UUID, keys []string, version int64) (*domain.Instance, error) {
	r.w.record("Instances.UpdateCurrentNodeKeys", tenant, id)
	r.w.nodeWrites = append(r.w.nodeWrites, wkactNodeWrite{Tenant: tenant, ID: id, Keys: keys, Version: version})
	inst, ok := r.w.instances[id]
	if !ok || inst.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.CurrentNodeKeys = keys
	inst.RecordVersion++
	return inst, nil
}

type wkactTasks struct {
	port.TaskRepository
	w *wkactWorld
}

func (r wkactTasks) GetByID(_ context.Context, tenant, id uuid.UUID) (*domain.Task, error) {
	r.w.record("Tasks.GetByID", tenant, id)
	task, ok := r.w.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	return task, nil
}

func (r wkactTasks) UpdateStatus(_ context.Context, tenant, id uuid.UUID, status domain.TaskStatus, version int64) (*domain.Task, error) {
	r.w.record("Tasks.UpdateStatus", tenant, id)
	task, ok := r.w.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	task.Status = status
	task.RecordVersion++
	return task, nil
}

func (r wkactTasks) BumpRecordVersion(_ context.Context, tenant, id uuid.UUID, version int64) (*domain.Task, error) {
	r.w.record("Tasks.BumpRecordVersion", tenant, id)
	task, ok := r.w.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	task.RecordVersion++
	return task, nil
}

func (r wkactTasks) ListByInstance(_ context.Context, tenant, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.w.record("Tasks.ListByInstance", tenant, instanceID)
	var out []*domain.Task
	for _, task := range r.w.tasks {
		if task.TenantID == tenant && task.WorkflowInstanceID == instanceID {
			out = append(out, task)
		}
	}
	slices.SortFunc(out, func(a, b *domain.Task) int { return compareStrings(a.NodeKey, b.NodeKey) })
	return out, nil, nil
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

type wkactAssignments struct {
	port.TaskAssignmentRepository
	w *wkactWorld
}

func (r wkactAssignments) GetByID(_ context.Context, tenant, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.w.record("Assignments.GetByID", tenant, id)
	a, ok := r.w.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	return a, nil
}

func (r wkactAssignments) ListActiveByTask(_ context.Context, tenant, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.w.record("Assignments.ListActiveByTask", tenant, taskID)
	var out []*domain.TaskAssignment
	for _, a := range r.w.assignments {
		if a.TenantID == tenant && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	slices.SortFunc(out, func(a, b *domain.TaskAssignment) int { return compareStrings(a.UserID.String(), b.UserID.String()) })
	return out, nil
}

func (r wkactAssignments) Vacate(_ context.Context, tenant, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.w.record("Assignments.Vacate", tenant, id)
	a, ok := r.w.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	a.IsActive = false
	return a, nil
}

func (r wkactAssignments) Complete(_ context.Context, tenant, id uuid.UUID, result json.RawMessage, version int64) (*domain.TaskAssignment, error) {
	r.w.recordVersioned("Assignments.Complete", tenant, id, version)
	a, ok := r.w.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	now := time.Unix(1_700_000_000, 0)
	a.IsActive = false
	a.ResultJSON = result
	a.CompletedAt = &now
	return a, nil
}

func (r wkactAssignments) SetLead(_ context.Context, tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	r.w.recordVersioned("Assignments.SetLead", tenant, taskID, version)
	a, ok := r.w.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	a.IsLead = true
	return a, nil
}

func (r wkactAssignments) Create(_ context.Context, a *domain.TaskAssignment) error {
	r.w.record("Assignments.Create", a.TenantID, a.TaskID)
	a.IsActive = true
	r.w.assignments[a.ID] = a
	return nil
}

type wkactOutbox struct {
	port.OutboxRepository
	w *wkactWorld
}

func (o wkactOutbox) Enqueue(_ context.Context, env events.Envelope[json.RawMessage]) error {
	o.w.record("Outbox.Enqueue", uuid.MustParse(env.TenantID), uuid.MustParse(strings.TrimPrefix(strings.TrimPrefix(env.Subject, "instances/"), "tasks/")))
	if err := o.w.fail("Outbox.Enqueue"); err != nil {
		return err
	}
	o.w.outbox = append(o.w.outbox, env)
	return nil
}

func (o wkactOutbox) ExistsForTask(_ context.Context, eventType string, taskID uuid.UUID) (bool, error) {
	for _, env := range o.w.outbox {
		if env.Type != eventType {
			continue
		}
		var data struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(env.Payload, &data); err == nil && data.TaskID == taskID.String() {
			return true, nil
		}
	}
	return false, nil
}

// wkactValidator checks payloads against the real event schemas unless err is
// set, in which case it refuses every payload.
type wkactValidator struct {
	real port.EventValidator
	err  error
}

func (v wkactValidator) Validate(ctx context.Context, eventType string, payload json.RawMessage) error {
	if v.err != nil {
		return v.err
	}
	return v.real.Validate(ctx, eventType, payload)
}

func wkactNewValidator(t *testing.T) wkactValidator {
	t.Helper()
	real, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	return wkactValidator{real: real}
}

func wkactNewDeps(t *testing.T, w *wkactWorld) (*outboundtemporal.Deps, *wkactTransactor) {
	t.Helper()
	tx := &wkactTransactor{world: w}
	return &outboundtemporal.Deps{
		Instances:   wkactInstances{w: w},
		Tasks:       wkactTasks{w: w},
		Assignments: wkactAssignments{w: w},
		Outbox:      wkactOutbox{w: w},
		Transactor:  tx,
		Validator:   wkactNewValidator(t),
	}, tx
}

// wkactPayloadFields decodes an enqueued payload into its raw top-level
// fields, so a test can tell [] from null.
func wkactPayloadFields(t *testing.T, env events.Envelope[json.RawMessage]) map[string]json.RawMessage {
	t.Helper()
	fields := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(env.Payload, &fields))
	return fields
}
