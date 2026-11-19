package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
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

var errW25bStorage = errors.New("w25b storage failure")

// w25bClock is the instant every write stamps, as the database stamps now().
var w25bClock = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// w25bSchemaValidator builds the real event-schema validator once for the package.
var w25bSchemaValidator = sync.OnceValues(eventbus.NewSchemaValidator)

// w25bCall is one repository call as the rig saw it.
type w25bCall struct {
	Op      string
	Tenant  uuid.UUID
	ID      uuid.UUID
	Version int64
	InTx    bool
	GUCSet  bool
	GUC     string
}

// w25bFailure makes one operation fail: on its nth call (1-based), or on every
// call when onCall is zero.
type w25bFailure struct {
	err    error
	onCall int
}

type w25bNodeWrite struct {
	Tenant  uuid.UUID
	ID      uuid.UUID
	Keys    []string
	Version int64
}

type w25bSnapshot struct {
	instances       map[uuid.UUID]domain.Instance
	tasks           map[uuid.UUID]domain.Task
	assignments     map[uuid.UUID]domain.TaskAssignment
	taskOrder       int
	assignmentOrder int
	outbox          int
}

type w25bTxOpen struct {
	GUCSet bool
	Tenant string
}

// w25bRig is the in-memory store behind every fake of one test. Its transactor
// rolls the whole store back when the callback fails, as the real transactor
// does. Repositories hand out copies, as a database does.
type w25bRig struct {
	instances       map[uuid.UUID]*domain.Instance
	tasks           map[uuid.UUID]*domain.Task
	taskOrder       []uuid.UUID
	assignments     map[uuid.UUID]*domain.TaskAssignment
	assignmentOrder []uuid.UUID
	outbox          []events.Envelope[json.RawMessage]
	nodeWrites      []w25bNodeWrite

	calls    []w25bCall
	counts   map[string]int
	failures map[string]w25bFailure

	txOpened   []w25bTxOpen
	committed  int
	rolledBack int
	inTx       bool
}

func w25bNewRig() *w25bRig {
	return &w25bRig{
		instances:   map[uuid.UUID]*domain.Instance{},
		tasks:       map[uuid.UUID]*domain.Task{},
		assignments: map[uuid.UUID]*domain.TaskAssignment{},
		counts:      map[string]int{},
		failures:    map[string]w25bFailure{},
	}
}

func (r *w25bRig) addInstance(tenant uuid.UUID, status domain.InstanceStatus) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenant, WorkflowVersionID: uuid.New(), SubjectID: "subject-" + uuid.NewString(),
		BusinessKey: "bk-" + uuid.NewString(), Status: status, StartedByUserID: uuid.New(), RecordVersion: 3,
	}
	r.instances[inst.ID] = inst
	return inst
}

func (r *w25bRig) addTask(tenant uuid.UUID, inst *domain.Instance, nodeKey string, status domain.TaskStatus) *domain.Task {
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, NodeKey: nodeKey, DepartmentID: uuid.New(),
		Status: status, RecordVersion: 7,
	}
	r.tasks[task.ID] = task
	r.taskOrder = append(r.taskOrder, task.ID)
	return task
}

func (r *w25bRig) addAssignment(tenant uuid.UUID, task *domain.Task, userID uuid.UUID, lead, active bool) *domain.TaskAssignment {
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenant, TaskID: task.ID, UserID: userID, IsLead: lead, IsActive: active}
	r.assignments[a.ID] = a
	r.assignmentOrder = append(r.assignmentOrder, a.ID)
	return a
}

func (r *w25bRig) failOn(op string, err error, onCall int) {
	r.failures[op] = w25bFailure{err: err, onCall: onCall}
}

// enter records the call, then reports the failure planned for it, if any.
func (r *w25bRig) enter(ctx context.Context, op string, tenant, id uuid.UUID, version int64) error {
	g, ok := pgcommon.GUCSetFromContext(ctx)
	r.calls = append(r.calls, w25bCall{Op: op, Tenant: tenant, ID: id, Version: version, InTx: r.inTx, GUCSet: ok, GUC: g.TenantID})
	r.counts[op]++
	if f, planned := r.failures[op]; planned && (f.onCall == 0 || f.onCall == r.counts[op]) {
		return f.err
	}
	return nil
}

func (r *w25bRig) callsTo(op string) []w25bCall {
	var out []w25bCall
	for _, c := range r.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func (r *w25bRig) idsOf(op string) []uuid.UUID {
	var out []uuid.UUID
	for _, c := range r.callsTo(op) {
		out = append(out, c.ID)
	}
	return out
}

func (r *w25bRig) eventsOfType(eventType string) []events.Envelope[json.RawMessage] {
	var out []events.Envelope[json.RawMessage]
	for _, env := range r.outbox {
		if env.Type == eventType {
			out = append(out, env)
		}
	}
	return out
}

// requireEveryCallScopedTo checks that each repository call ran inside a
// transaction opened on a context that already carried the tenant, and that
// the call itself was made for that tenant.
func (r *w25bRig) requireEveryCallScopedTo(t *testing.T, tenant uuid.UUID) {
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

func (r *w25bRig) snapshot() w25bSnapshot {
	s := w25bSnapshot{
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

func (r *w25bRig) restore(s w25bSnapshot) {
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

type w25bTransactor struct{ rig *w25bRig }

func (x w25bTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	r := x.rig
	g, ok := pgcommon.GUCSetFromContext(ctx)
	r.txOpened = append(r.txOpened, w25bTxOpen{GUCSet: ok, Tenant: g.TenantID})
	snap := r.snapshot()
	r.inTx = true
	err := fn(ctx)
	r.inTx = false
	if err != nil {
		r.restore(snap)
		r.rolledBack++
		return err
	}
	r.committed++
	return nil
}

func (x w25bTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return x.RunInTx(ctx, fn)
}

type w25bInstances struct {
	port.InstanceRepository
	rig *w25bRig
}

func (r w25bInstances) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.Instance, error) {
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

func (r w25bInstances) UpdateStatus(ctx context.Context, tenant, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
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
	stamp := w25bClock
	inst.UpdatedAt = stamp
	if status == domain.InstanceStatusCompleted || status == domain.InstanceStatusTerminated || status == domain.InstanceStatusFailed {
		inst.CompletedAt = &stamp
	}
	cp := *inst
	return &cp, nil
}

func (r w25bInstances) UpdateCurrentNodeKeys(ctx context.Context, tenant, id uuid.UUID, keys []string, version int64) (*domain.Instance, error) {
	if err := r.rig.enter(ctx, "Instances.UpdateCurrentNodeKeys", tenant, id, version); err != nil {
		return nil, err
	}
	r.rig.nodeWrites = append(r.rig.nodeWrites, w25bNodeWrite{Tenant: tenant, ID: id, Keys: keys, Version: version})
	inst, ok := r.rig.instances[id]
	if !ok || inst.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.CurrentNodeKeys = keys
	inst.RecordVersion++
	inst.UpdatedAt = w25bClock
	cp := *inst
	return &cp, nil
}

type w25bTasks struct {
	port.TaskRepository
	rig *w25bRig
}

func (r w25bTasks) Create(ctx context.Context, task *domain.Task) error {
	if err := r.rig.enter(ctx, "Tasks.Create", task.TenantID, task.ID, 0); err != nil {
		return err
	}
	if inst, ok := r.rig.instances[task.WorkflowInstanceID]; !ok || inst.TenantID != task.TenantID {
		return domain.ErrNotFound
	}
	if _, exists := r.rig.tasks[task.ID]; exists {
		return domain.ErrAlreadyExists
	}
	stored := *task
	stored.RecordVersion = 1
	stored.CreatedAt = w25bClock
	stored.UpdatedAt = w25bClock
	r.rig.tasks[stored.ID] = &stored
	r.rig.taskOrder = append(r.rig.taskOrder, stored.ID)
	return nil
}

func (r w25bTasks) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.Task, error) {
	if err := r.rig.enter(ctx, "Tasks.GetByID", tenant, id, 0); err != nil {
		return nil, err
	}
	task, ok := r.rig.tasks[id]
	if !ok || task.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	cp := *task
	return &cp, nil
}

func (r w25bTasks) UpdateStatus(ctx context.Context, tenant, id uuid.UUID, status domain.TaskStatus, version int64) (*domain.Task, error) {
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
	task.UpdatedAt = w25bClock
	switch status {
	case domain.TaskStatusCompleted, domain.TaskStatusDeferred, domain.TaskStatusFailed, domain.TaskStatusSuperseded:
		stamp := w25bClock
		task.CompletedAt = &stamp
	case domain.TaskStatusReady, domain.TaskStatusInProgress:
	}
	cp := *task
	return &cp, nil
}

func (r w25bTasks) BumpRecordVersion(ctx context.Context, tenant, id uuid.UUID, version int64) (*domain.Task, error) {
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
	task.UpdatedAt = w25bClock
	cp := *task
	return &cp, nil
}

// ListByInstance serves the instance's tasks in creation order, one page of at
// most page.Limit rows at a time, with a cursor to the page after.
func (r w25bTasks) ListByInstance(ctx context.Context, tenant, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
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
	return all[start:end], next, nil
}

type w25bAssignments struct {
	port.TaskAssignmentRepository
	rig *w25bRig
}

func (r w25bAssignments) GetByID(ctx context.Context, tenant, id uuid.UUID) (*domain.TaskAssignment, error) {
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

func (r w25bAssignments) ListActiveByTask(ctx context.Context, tenant, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
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

func (r w25bAssignments) Vacate(ctx context.Context, tenant, id uuid.UUID) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.Vacate", tenant, id, 0); err != nil {
		return nil, err
	}
	a, ok := r.rig.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	stamp := w25bClock
	a.IsActive = false
	a.VacatedAt = &stamp
	a.UpdatedAt = stamp
	cp := *a
	return &cp, nil
}

// Complete closes the assignment and bumps the task's record version, failing
// when the version the caller read is out of date.
func (r w25bAssignments) Complete(ctx context.Context, tenant, id uuid.UUID, result json.RawMessage, version int64) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.Complete", tenant, id, version); err != nil {
		return nil, err
	}
	a, ok := r.rig.assignments[id]
	if !ok || a.TenantID != tenant {
		return nil, domain.ErrNotFound
	}
	task := r.rig.tasks[a.TaskID]
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	task.RecordVersion++
	done := w25bClock
	a.IsActive = false
	a.ResultJSON = result
	a.CompletedAt = &done
	a.UpdatedAt = done
	cp := *a
	return &cp, nil
}

func (r w25bAssignments) ClaimLead(ctx context.Context, tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.ClaimLead", tenant, id, version); err != nil {
		return nil, err
	}
	return r.setLead(tenant, taskID, id, version)
}

func (r w25bAssignments) SetLead(ctx context.Context, tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.SetLead", tenant, id, version); err != nil {
		return nil, err
	}
	return r.setLead(tenant, taskID, id, version)
}

// setLead makes one assignment the task's only lead and bumps the task's
// record version, failing when the version the caller read is out of date.
func (r w25bAssignments) setLead(tenant, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
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
	a.UpdatedAt = w25bClock
	task.RecordVersion++
	cp := *a
	return &cp, nil
}

func (r w25bAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	if err := r.rig.enter(ctx, "Assignments.Create", a.TenantID, a.ID, 0); err != nil {
		return err
	}
	if task, ok := r.rig.tasks[a.TaskID]; !ok || task.TenantID != a.TenantID {
		return domain.ErrNotFound
	}
	if _, exists := r.rig.assignments[a.ID]; exists {
		return domain.ErrAlreadyExists
	}
	for _, other := range r.rig.assignments {
		if other.TaskID == a.TaskID && other.UserID == a.UserID && other.IsActive {
			return domain.ErrDuplicateActiveAssignment
		}
	}
	stored := *a
	stored.IsActive = true
	assignedAt := w25bClock
	stored.AssignedAt = &assignedAt
	stored.UpdatedAt = w25bClock
	r.rig.assignments[stored.ID] = &stored
	r.rig.assignmentOrder = append(r.rig.assignmentOrder, stored.ID)
	return nil
}

type w25bOutbox struct {
	port.OutboxRepository
	rig *w25bRig
}

func (o w25bOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	tenant, _ := uuid.Parse(env.TenantID)
	id, _ := uuid.Parse(env.ID)
	if err := o.rig.enter(ctx, "Outbox.Enqueue", tenant, id, 0); err != nil {
		return err
	}
	o.rig.outbox = append(o.rig.outbox, env)
	return nil
}

func (o w25bOutbox) ExistsByID(ctx context.Context, tenant, id uuid.UUID) (bool, error) {
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

func (o w25bOutbox) ExistsForTask(ctx context.Context, eventType string, taskID uuid.UUID) (bool, error) {
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

// w25bValidator checks payloads against the real event schemas unless err is
// set, in which case it refuses every payload.
type w25bValidator struct {
	real port.EventValidator
	err  error
}

func (v w25bValidator) Validate(ctx context.Context, eventType string, payload json.RawMessage) error {
	if v.err != nil {
		return v.err
	}
	return v.real.Validate(ctx, eventType, payload)
}

func w25bNewDeps(t *testing.T, rig *w25bRig) *outboundtemporal.Deps {
	t.Helper()
	real, err := w25bSchemaValidator()
	require.NoError(t, err)
	return &outboundtemporal.Deps{
		Instances:   w25bInstances{rig: rig},
		Tasks:       w25bTasks{rig: rig},
		Assignments: w25bAssignments{rig: rig},
		Outbox:      w25bOutbox{rig: rig},
		Transactor:  w25bTransactor{rig: rig},
		Validator:   w25bValidator{real: real},
	}
}

// w25bRejectEvents makes the deps' validator refuse every event payload.
func w25bRejectEvents(deps *outboundtemporal.Deps, err error) {
	v, _ := deps.Validator.(w25bValidator)
	v.err = err
	deps.Validator = v
}

// w25bRequireNonRetryable checks that the activity ended with a Temporal
// application error of the given type that Temporal will not retry, and that
// the message names the field the activity refused.
func w25bRequireNonRetryable(t *testing.T, err error, errType, messagePart string) *temporal.ApplicationError {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, errType, appErr.Type())
	require.True(t, appErr.NonRetryable())
	require.Contains(t, appErr.Message(), messagePart)
	return appErr
}

// w25bRequireRetryable checks that the error is not a Temporal application
// error at all, so Temporal retries the activity under its retry policy.
func w25bRequireRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.False(t, errors.As(err, &appErr), "expected a plain retryable error, got application error %v", err)
}

func w25bDecode[T any](t *testing.T, env events.Envelope[json.RawMessage]) T {
	t.Helper()
	var out T
	require.NoError(t, json.Unmarshal(env.Payload, &out))
	return out
}

// w25bRequireUntouched checks that the rig holds exactly what the snapshot held.
func w25bRequireUntouched(t *testing.T, rig *w25bRig, before w25bSnapshot) {
	t.Helper()
	require.Equal(t, before, rig.snapshot())
}
