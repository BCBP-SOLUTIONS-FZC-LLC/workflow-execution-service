package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w22aPageSize is the page size the activities under test ask the task
// repository for; the rig honours it the way the real repository does.
const w22aPageSize = 50

var (
	errW22aStorage = errors.New("w22a storage failure")
	errW22aCommit  = errors.New("w22a commit failure")
)

// w22aCall is one repository call as the rig saw it: the operation, the tenant
// and resource it was given, the record version it carried, and what the
// context held when it arrived.
type w22aCall struct {
	Op      string
	Tenant  uuid.UUID
	ID      uuid.UUID
	Version int64
	InTx    bool
	GUCSet  bool
	GUC     string
}

// w22aFailure makes one operation fail: on its nth call (1-based), or on every
// call when onCall is zero.
type w22aFailure struct {
	err    error
	onCall int
}

type w22aSnapshot struct {
	instances       map[uuid.UUID]domain.Instance
	tasks           map[uuid.UUID]domain.Task
	assignments     map[uuid.UUID]domain.TaskAssignment
	taskOrder       int
	assignmentOrder int
	outbox          int
}

type w22aTxOpen struct {
	GUCSet bool
	Tenant string
}

// w22aRig is the in-memory store behind every fake of one test. Its
// transactor rolls the whole store back when the callback fails or the commit
// is refused, as the real transactor does. Repositories hand out copies, as a
// database does.
type w22aRig struct {
	instances       map[uuid.UUID]*domain.Instance
	tasks           map[uuid.UUID]*domain.Task
	taskOrder       []uuid.UUID
	assignments     map[uuid.UUID]*domain.TaskAssignment
	assignmentOrder []uuid.UUID
	outbox          []events.Envelope[json.RawMessage]

	calls        []w22aCall
	counts       map[string]int
	failures     map[string]w22aFailure
	pageRequests []port.PageRequest
	// afterRead runs once, right after the named operation has read its rows:
	// a concurrent writer that lands between the read and the write.
	afterRead     map[string]func()
	outsideWrites []func()

	commitErr  error
	txOpened   []w22aTxOpen
	committed  int
	rolledBack int
	inTx       bool
	clock      time.Time
}

func w22aNewRig() *w22aRig {
	return &w22aRig{
		instances:   map[uuid.UUID]*domain.Instance{},
		tasks:       map[uuid.UUID]*domain.Task{},
		assignments: map[uuid.UUID]*domain.TaskAssignment{},
		counts:      map[string]int{},
		failures:    map[string]w22aFailure{},
		afterRead:   map[string]func(){},
		clock:       time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC),
	}
}

func (r *w22aRig) tick() time.Time {
	r.clock = r.clock.Add(time.Minute)
	return r.clock
}

func (r *w22aRig) addInstance(tenant uuid.UUID, status domain.InstanceStatus) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenant, WorkflowVersionID: uuid.New(), SubjectID: "subject-" + uuid.NewString(),
		BusinessKey: "bk-" + uuid.NewString(), Status: status, StartedByUserID: uuid.New(), RecordVersion: 3,
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	r.instances[inst.ID] = inst
	return inst
}

func (r *w22aRig) addTask(tenant uuid.UUID, inst *domain.Instance, nodeKey string, status domain.TaskStatus) *domain.Task {
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, NodeKey: nodeKey, DepartmentID: uuid.New(),
		Status: status, RecordVersion: 7,
	}
	r.tasks[task.ID] = task
	r.taskOrder = append(r.taskOrder, task.ID)
	return task
}

func (r *w22aRig) addAssignment(tenant uuid.UUID, task *domain.Task, userID uuid.UUID, lead, active bool) *domain.TaskAssignment {
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenant, TaskID: task.ID, UserID: userID, IsLead: lead, IsActive: active}
	r.assignments[a.ID] = a
	r.assignmentOrder = append(r.assignmentOrder, a.ID)
	return a
}

func (r *w22aRig) failOn(op string, err error, onCall int) {
	r.failures[op] = w22aFailure{err: err, onCall: onCall}
}

// enter records the call, then reports the failure planned for it, if any.
func (r *w22aRig) enter(ctx context.Context, op string, tenant, id uuid.UUID, version int64) error {
	g, ok := pgcommon.GUCSetFromContext(ctx)
	r.calls = append(r.calls, w22aCall{Op: op, Tenant: tenant, ID: id, Version: version, InTx: r.inTx, GUCSet: ok, GUC: g.TenantID})
	r.counts[op]++
	if f, planned := r.failures[op]; planned && (f.onCall == 0 || f.onCall == r.counts[op]) {
		return f.err
	}
	return nil
}

// runAfterRead plays the concurrent writer planned for the operation. Its write
// belongs to another transaction, so it survives the rollback of this one.
func (r *w22aRig) runAfterRead(op string) {
	if hook, ok := r.afterRead[op]; ok {
		delete(r.afterRead, op)
		hook()
		r.outsideWrites = append(r.outsideWrites, hook)
	}
}

func (r *w22aRig) callsTo(op string) []w22aCall {
	var out []w22aCall
	for _, c := range r.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func (r *w22aRig) idsOf(op string) []uuid.UUID {
	var out []uuid.UUID
	for _, c := range r.callsTo(op) {
		out = append(out, c.ID)
	}
	return out
}

func (r *w22aRig) ops() []string {
	out := make([]string, 0, len(r.calls))
	for _, c := range r.calls {
		out = append(out, c.Op)
	}
	return out
}

func (r *w22aRig) eventsOfType(eventType string) []events.Envelope[json.RawMessage] {
	var out []events.Envelope[json.RawMessage]
	for _, env := range r.outbox {
		if env.Type == eventType {
			out = append(out, env)
		}
	}
	return out
}

func (r *w22aRig) eventTypes() []string {
	out := make([]string, 0, len(r.outbox))
	for _, env := range r.outbox {
		out = append(out, env.Type)
	}
	return out
}

// requireEveryCallScopedTo checks that each repository call ran inside a
// transaction opened on a context that already carried the tenant, and that
// the call itself was made for that tenant.
func (r *w22aRig) requireEveryCallScopedTo(t *testing.T, tenant uuid.UUID) {
	t.Helper()
	require.NotEmpty(t, r.calls)
	require.NotEmpty(t, r.txOpened)
	for _, tx := range r.txOpened {
		require.True(t, tx.GUCSet, "the transaction was opened without a tenant GUC on the context")
		require.Equal(t, tenant.String(), tx.Tenant)
	}
	for _, c := range r.calls {
		require.True(t, c.InTx, "%s ran outside the transaction", c.Op)
		require.True(t, c.GUCSet, "%s ran without the tenant GUC", c.Op)
		require.Equal(t, tenant.String(), c.GUC, "%s ran under another tenant's GUC", c.Op)
		if c.Op != "Outbox.ExistsForTask" {
			require.Equal(t, tenant, c.Tenant, "%s was given another tenant", c.Op)
		}
	}
}

func (r *w22aRig) snapshot() w22aSnapshot {
	s := w22aSnapshot{
		instances:       map[uuid.UUID]domain.Instance{},
		tasks:           map[uuid.UUID]domain.Task{},
		assignments:     map[uuid.UUID]domain.TaskAssignment{},
		taskOrder:       len(r.taskOrder),
		assignmentOrder: len(r.assignmentOrder),
		outbox:          len(r.outbox),
	}
	for id, v := range r.instances {
		s.instances[id] = *v
	}
	for id, v := range r.tasks {
		s.tasks[id] = *v
	}
	for id, v := range r.assignments {
		s.assignments[id] = *v
	}
	return s
}

func (r *w22aRig) restore(s w22aSnapshot) {
	for id, p := range r.instances {
		if v, ok := s.instances[id]; ok {
			*p = v
		} else {
			delete(r.instances, id)
		}
	}
	for id, p := range r.tasks {
		if v, ok := s.tasks[id]; ok {
			*p = v
		} else {
			delete(r.tasks, id)
		}
	}
	for id, p := range r.assignments {
		if v, ok := s.assignments[id]; ok {
			*p = v
		} else {
			delete(r.assignments, id)
		}
	}
	r.taskOrder = r.taskOrder[:s.taskOrder]
	r.assignmentOrder = r.assignmentOrder[:s.assignmentOrder]
	r.outbox = r.outbox[:s.outbox]
}

type w22aTransactor struct{ rig *w22aRig }

func (x w22aTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	r := x.rig
	g, ok := pgcommon.GUCSetFromContext(ctx)
	r.txOpened = append(r.txOpened, w22aTxOpen{GUCSet: ok, Tenant: g.TenantID})
	snap := r.snapshot()
	r.inTx = true
	err := fn(ctx)
	r.inTx = false
	if err == nil && r.commitErr != nil {
		err = r.commitErr
	}
	pending := r.outsideWrites
	r.outsideWrites = nil
	if err != nil {
		r.restore(snap)
		for _, write := range pending {
			write()
		}
		r.rolledBack++
		return err
	}
	r.committed++
	return nil
}

func (x w22aTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return x.RunInTx(ctx, fn)
}

type w22aInstances struct {
	port.InstanceRepository
	rig *w22aRig
}

func (r w22aInstances) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.Instance, error) {
	if err := r.rig.enter(ctx, "Instances.GetByID", tenant, id, 0); err != nil {
		return nil, err
	}
	inst, ok := r.rig.instances[id]
	if !ok || inst.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	cp := *inst
	return &cp, nil
}

func (r w22aInstances) UpdateStatus(ctx context.Context, tenant, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
	if err := r.rig.enter(ctx, "Instances.UpdateStatus", tenant, id, version); err != nil {
		return nil, err
	}
	inst, ok := r.rig.instances[id]
	if !ok || inst.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.Status = status
	inst.RecordVersion++
	inst.UpdatedAt = r.rig.tick()
	cp := *inst
	return &cp, nil
}

type w22aTasks struct {
	port.TaskRepository
	rig *w22aRig
}

func (r w22aTasks) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.Task, error) {
	if err := r.rig.enter(ctx, "Tasks.GetByID", tenant, id, 0); err != nil {
		return nil, err
	}
	task, ok := r.rig.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	cp := *task
	r.rig.runAfterRead("Tasks.GetByID")
	return &cp, nil
}

func (r w22aTasks) UpdateStatus(ctx context.Context, tenant, id uuid.UUID, status domain.TaskStatus, version int64) (*domain.Task, error) {
	if err := r.rig.enter(ctx, "Tasks.UpdateStatus", tenant, id, version); err != nil {
		return nil, err
	}
	task, ok := r.rig.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	task.Status = status
	task.RecordVersion++
	cp := *task
	return &cp, nil
}

func (r w22aTasks) BumpRecordVersion(ctx context.Context, tenant, id uuid.UUID, version int64) (*domain.Task, error) {
	if err := r.rig.enter(ctx, "Tasks.BumpRecordVersion", tenant, id, version); err != nil {
		return nil, err
	}
	task, ok := r.rig.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	task.RecordVersion++
	cp := *task
	return &cp, nil
}

// ListByInstance serves the instance's tasks in creation order, one page of at
// most page.Limit rows at a time, with a cursor to the page after.
func (r w22aTasks) ListByInstance(ctx context.Context, tenant, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.rig.pageRequests = append(r.rig.pageRequests, page)
	if err := r.rig.enter(ctx, "Tasks.ListByInstance", tenant, instanceID, 0); err != nil {
		return nil, nil, err
	}
	var all []*domain.Task
	for _, id := range r.rig.taskOrder {
		if task := r.rig.tasks[id]; task.TenantID == tenant && task.WorkflowInstanceID == instanceID {
			cp := *task
			all = append(all, &cp)
		}
	}
	start := 0
	if page.After != nil {
		for i, task := range all {
			if task.ID == page.After.ID {
				start = i + 1
			}
		}
	}
	end := len(all)
	var next *port.Cursor
	if page.Limit > 0 && start+page.Limit < end {
		end = start + page.Limit
		next = &port.Cursor{ID: all[end-1].ID}
	}
	out := all[start:end]
	r.rig.runAfterRead("Tasks.ListByInstance")
	return out, next, nil
}

type w22aAssignments struct {
	port.TaskAssignmentRepository
	rig *w22aRig
}

func (r w22aAssignments) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.GetByID", tenant, id, 0); err != nil {
		return nil, err
	}
	a, ok := r.rig.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	cp := *a
	return &cp, nil
}

func (r w22aAssignments) ListActiveByTask(ctx context.Context, tenant, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.ListActiveByTask", tenant, taskID, 0); err != nil {
		return nil, err
	}
	var out []*domain.TaskAssignment
	for _, id := range r.rig.assignmentOrder {
		if a := r.rig.assignments[id]; a.TenantID == tenant && a.TaskID == taskID && a.IsActive {
			cp := *a
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r w22aAssignments) Vacate(ctx context.Context, tenant, id uuid.UUID) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.Vacate", tenant, id, 0); err != nil {
		return nil, err
	}
	a, ok := r.rig.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	a.IsActive = false
	cp := *a
	return &cp, nil
}

func (r w22aAssignments) ClaimLead(ctx context.Context, tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.ClaimLead", tenant, id, version); err != nil {
		return nil, err
	}
	return r.setLead(tenant, taskID, id, version)
}

func (r w22aAssignments) SetLead(ctx context.Context, tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.SetLead", tenant, id, version); err != nil {
		return nil, err
	}
	return r.setLead(tenant, taskID, id, version)
}

// setLead makes one assignment the task's only lead and bumps the task's
// record version, failing when the version the caller read is out of date.
func (r w22aAssignments) setLead(tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	task, ok := r.rig.tasks[taskID]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	a, ok := r.rig.assignments[id]
	if !ok || a.TenantID != tenant || a.TaskID != taskID {
		return nil, domain.ErrNotFound
	}
	for _, other := range r.rig.assignments {
		if other.TaskID == taskID {
			other.IsLead = false
		}
	}
	a.IsLead = true
	task.RecordVersion++
	cp := *a
	return &cp, nil
}

func (r w22aAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	if err := r.rig.enter(ctx, "Assignments.Create", a.TenantID, a.ID, 0); err != nil {
		return err
	}
	if _, exists := r.rig.assignments[a.ID]; exists {
		return domain.ErrAlreadyExists
	}
	stored := *a
	stored.IsActive = true
	r.rig.assignments[stored.ID] = &stored
	r.rig.assignmentOrder = append(r.rig.assignmentOrder, stored.ID)
	return nil
}

type w22aOutbox struct {
	port.OutboxRepository
	rig *w22aRig
}

func (o w22aOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	tenant, _ := uuid.Parse(env.TenantID)
	id, _ := uuid.Parse(env.ID)
	if err := o.rig.enter(ctx, "Outbox.Enqueue", tenant, id, 0); err != nil {
		return err
	}
	o.rig.outbox = append(o.rig.outbox, env)
	return nil
}

func (o w22aOutbox) ExistsByID(ctx context.Context, tenant, id uuid.UUID) (bool, error) {
	if err := o.rig.enter(ctx, "Outbox.ExistsByID", tenant, id, 0); err != nil {
		return false, err
	}
	for _, env := range o.rig.outbox {
		if env.ID == id.String() && env.TenantID == tenant.String() {
			return true, nil
		}
	}
	return false, nil
}

func (o w22aOutbox) ExistsForTask(ctx context.Context, eventType string, taskID uuid.UUID) (bool, error) {
	if err := o.rig.enter(ctx, "Outbox.ExistsForTask", uuid.Nil, taskID, 0); err != nil {
		return false, err
	}
	for _, env := range o.rig.outbox {
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

func w22aNewDeps(t *testing.T, rig *w22aRig) *outboundtemporal.Deps {
	t.Helper()
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	return &outboundtemporal.Deps{
		Instances:   w22aInstances{rig: rig},
		Tasks:       w22aTasks{rig: rig},
		Assignments: w22aAssignments{rig: rig},
		Outbox:      w22aOutbox{rig: rig},
		Transactor:  w22aTransactor{rig: rig},
		Validator:   validator,
	}
}

// w22aRequireNonRetryable checks that the activity ended with a Temporal
// application error of the given type that Temporal will not retry.
func w22aRequireNonRetryable(t *testing.T, err error, errType string) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, errType, appErr.Type())
	require.True(t, appErr.NonRetryable())
}

// w22aRequireRetryable checks that the error is not a Temporal application
// error at all, so Temporal retries the activity under its retry policy.
func w22aRequireRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.False(t, errors.As(err, &appErr), "expected a plain retryable error, got application error %v", err)
}

func w22aDecode[T any](t *testing.T, env events.Envelope[json.RawMessage]) T {
	t.Helper()
	var out T
	require.NoError(t, json.Unmarshal(env.Payload, &out))
	return out
}

func w22aRawFields(t *testing.T, env events.Envelope[json.RawMessage]) map[string]json.RawMessage {
	t.Helper()
	fields := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(env.Payload, &fields))
	return fields
}
