package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fakeInstanceRepo is an in-memory port.InstanceRepository — no real
// Postgres involved, this package's own tests exercise Activity logic only.
type fakeInstanceRepo struct {
	byID                     map[uuid.UUID]*domain.Instance
	updateStatusErr          error
	updateCurrentNodeKeysErr error
}

func newFakeInstanceRepo(instances ...*domain.Instance) *fakeInstanceRepo {
	m := make(map[uuid.UUID]*domain.Instance, len(instances))
	for _, inst := range instances {
		m[inst.ID] = inst
	}
	return &fakeInstanceRepo{byID: m}
}

func (r *fakeInstanceRepo) Create(_ context.Context, inst *domain.Instance) error {
	r.byID[inst.ID] = inst
	return nil
}

func (r *fakeInstanceRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.Instance, error) {
	inst, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return inst, nil
}

func (r *fakeInstanceRepo) UpdateStatus(_ context.Context, _, id uuid.UUID, status domain.InstanceStatus, recordVersion int64) (*domain.Instance, error) {
	if r.updateStatusErr != nil {
		return nil, r.updateStatusErr
	}
	inst, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.Status = status
	inst.RecordVersion++
	return inst, nil
}

func (r *fakeInstanceRepo) ListByTenant(_ context.Context, _ uuid.UUID, _ port.InstanceListFilter, _ port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	return nil, nil, nil
}

func (r *fakeInstanceRepo) CountActiveByWorkflow(_ context.Context, _, _ uuid.UUID) (int64, error) {
	return 0, nil
}

func (r *fakeInstanceRepo) CountActiveByTaskQueue(_ context.Context, _ uuid.UUID, _ string) (int64, error) {
	return 0, nil
}

func (r *fakeInstanceRepo) UpdateCurrentNodeKeys(_ context.Context, _, id uuid.UUID, keys []string, recordVersion int64) (*domain.Instance, error) {
	if r.updateCurrentNodeKeysErr != nil {
		return nil, r.updateCurrentNodeKeysErr
	}
	inst, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if inst.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	inst.CurrentNodeKeys = keys
	inst.RecordVersion++
	return inst, nil
}

// fakeTaskRepo is an in-memory port.TaskRepository. createErr, when set,
// makes Create fail without touching byID — used to exercise callers'
// DB-failure branches. getByIDErr, when set, makes GetByID fail for
// getByIDErrID specifically (or for every ID, if getByIDErrID is left
// uuid.Nil) — needed to force the post-ErrAlreadyExists GetByID error branch
// in createRegressionTask without also breaking the same call's earlier,
// unrelated GetByID(taskID) lookup. pages, when non-nil, makes ListByInstance
// ignore byID and instead walk through pages one PageRequest at a time —
// used to exercise a caller's multi-page cursor loop (byID's own
// ListByInstance has no real pagination, it returns everything in one page).
type fakeTaskRepo struct {
	byID            map[uuid.UUID]*domain.Task
	createErr       error
	updateStatusErr error
	getByIDErr      error
	getByIDErrID    uuid.UUID
	listErr         error
	pages           [][]*domain.Task
	pageCalls       int
}

func newFakeTaskRepo(tasks ...*domain.Task) *fakeTaskRepo {
	m := make(map[uuid.UUID]*domain.Task, len(tasks))
	for _, task := range tasks {
		m[task.ID] = task
	}
	return &fakeTaskRepo{byID: m}
}

func (r *fakeTaskRepo) Create(_ context.Context, task *domain.Task) error {
	if r.createErr != nil {
		return r.createErr
	}
	if _, exists := r.byID[task.ID]; exists {
		// Mirrors mapErr's real primary-key-conflict classification — a
		// retried CreateTask/DeferTask activity (deterministic ID) hits its
		// own row on a second attempt.
		return domain.ErrAlreadyExists
	}
	if task.AssigneeMode == "" {
		task.AssigneeMode = "single"
	}
	task.RecordVersion = 1
	r.byID[task.ID] = task
	return nil
}

func (r *fakeTaskRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.Task, error) {
	if r.getByIDErr != nil && (r.getByIDErrID == uuid.Nil || r.getByIDErrID == id) {
		return nil, r.getByIDErr
	}
	task, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return task, nil
}

func (r *fakeTaskRepo) UpdateStatus(_ context.Context, _, id uuid.UUID, status domain.TaskStatus, recordVersion int64) (*domain.Task, error) {
	if r.updateStatusErr != nil {
		return nil, r.updateStatusErr
	}
	task, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	task.Status = status
	task.RecordVersion++
	return task, nil
}

func (r *fakeTaskRepo) ListByInstance(_ context.Context, _, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	if r.listErr != nil {
		return nil, nil, r.listErr
	}
	if r.pages != nil {
		idx := r.pageCalls
		r.pageCalls++
		if idx >= len(r.pages) {
			return nil, nil, nil
		}
		var next *port.Cursor
		if idx < len(r.pages)-1 {
			next = &port.Cursor{}
		}
		return r.pages[idx], next, nil
	}
	var out []*domain.Task
	for _, task := range r.byID {
		if task.WorkflowInstanceID == instanceID {
			out = append(out, task)
		}
	}
	return out, nil, nil
}

func (r *fakeTaskRepo) ListByTenant(_ context.Context, _ uuid.UUID, _ port.TaskListFilter, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	if r.listErr != nil {
		return nil, nil, r.listErr
	}
	return nil, nil, nil
}

func (r *fakeTaskRepo) GetByInstanceAndNode(_ context.Context, _, instanceID uuid.UUID, nodeKey string) (*domain.Task, error) {
	for _, task := range r.byID {
		if task.WorkflowInstanceID == instanceID && task.NodeKey == nodeKey {
			return task, nil
		}
	}
	return nil, domain.ErrNotFound
}

// fakeAssignmentRepo is an in-memory port.TaskAssignmentRepository. createErr,
// when set, makes Create fail — used to exercise callers' DB-failure branches.
type fakeAssignmentRepo struct {
	byID          map[uuid.UUID]*domain.TaskAssignment
	createErr     error
	getByIDErr    error
	setLeadErr    error
	vacateErr     error
	completeErr   error
	listActiveErr error

	// lastCompleteVersion/lastSetLeadVersion record the taskRecordVersion
	// each call received, for tests asserting the caller passes the freshly
	// fetched version rather than a stale one.
	lastCompleteVersion int64
	lastSetLeadVersion  int64
}

func newFakeAssignmentRepo(assignments ...*domain.TaskAssignment) *fakeAssignmentRepo {
	m := make(map[uuid.UUID]*domain.TaskAssignment, len(assignments))
	for _, a := range assignments {
		a.IsActive = true
		m[a.ID] = a
	}
	return &fakeAssignmentRepo{byID: m}
}

func (r *fakeAssignmentRepo) Create(_ context.Context, a *domain.TaskAssignment) error {
	if r.createErr != nil {
		return r.createErr
	}
	if _, exists := r.byID[a.ID]; exists {
		return domain.ErrAlreadyExists
	}
	a.IsActive = true
	r.byID[a.ID] = a
	return nil
}

func (r *fakeAssignmentRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.TaskAssignment, error) {
	if r.getByIDErr != nil {
		return nil, r.getByIDErr
	}
	a, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return a, nil
}

func (r *fakeAssignmentRepo) ListActiveByTask(_ context.Context, _, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if r.listActiveErr != nil {
		return nil, r.listActiveErr
	}
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeAssignmentRepo) ListActiveByUser(_ context.Context, _, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.UserID == userID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeAssignmentRepo) ListActiveByUserPaginated(_ context.Context, _, userID uuid.UUID, _ port.PageRequest) ([]port.ActiveUserTaskRow, *port.Cursor, error) {
	var out []port.ActiveUserTaskRow
	for _, a := range r.byID {
		if a.UserID == userID && a.IsActive {
			out = append(out, port.ActiveUserTaskRow{TaskID: a.TaskID, UserID: a.UserID})
		}
	}
	return out, nil, nil
}

func (r *fakeAssignmentRepo) VacateAllActiveByUser(_ context.Context, _, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.UserID == userID && a.IsActive {
			a.IsActive = false
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeAssignmentRepo) Vacate(_ context.Context, _, id uuid.UUID) (*domain.TaskAssignment, error) {
	if r.vacateErr != nil {
		return nil, r.vacateErr
	}
	a, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	a.IsActive = false
	return a, nil
}

func (r *fakeAssignmentRepo) Complete(_ context.Context, _, id uuid.UUID, resultJSON json.RawMessage, taskRecordVersion int64) (*domain.TaskAssignment, error) {
	r.lastCompleteVersion = taskRecordVersion
	if r.completeErr != nil {
		return nil, r.completeErr
	}
	a, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	now := time.Now()
	a.IsActive = false
	a.ResultJSON = resultJSON
	a.CompletedAt = &now
	return a, nil
}

func (r *fakeAssignmentRepo) SetLead(_ context.Context, _, _, id uuid.UUID, taskRecordVersion int64) (*domain.TaskAssignment, error) {
	r.lastSetLeadVersion = taskRecordVersion
	if r.setLeadErr != nil {
		return nil, r.setLeadErr
	}
	a, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	a.IsLead = true
	return a, nil
}

// fakeOutbox is an in-memory port.OutboxRepository — Enqueue just records
// every envelope it receives for assertions, no real transaction required.
type fakeOutbox struct {
	enqueued   []events.Envelope[json.RawMessage]
	enqueueErr error
	existsErr  error
}

func (o *fakeOutbox) Enqueue(_ context.Context, env events.Envelope[json.RawMessage]) error {
	if o.enqueueErr != nil {
		return o.enqueueErr
	}
	o.enqueued = append(o.enqueued, env)
	return nil
}

func (o *fakeOutbox) ListByInstance(_ context.Context, _, _ uuid.UUID, _ port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	return nil, nil, nil
}

func (o *fakeOutbox) ExistsForTask(_ context.Context, eventType string, taskID uuid.UUID) (bool, error) {
	if o.existsErr != nil {
		return false, o.existsErr
	}
	for _, env := range o.enqueued {
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

// fakeTransactor just runs fn directly against the given ctx — this
// package's fakes have no real transaction/connection to acquire.
type fakeTransactor struct{}

func (fakeTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (fakeTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// noopValidator always accepts — this package's tests assert on repo/outbox
// state and payload construction, not schema validation itself (already
// covered by test/unit/eventbus).
type noopValidator struct{}

func (noopValidator) Validate(context.Context, string, json.RawMessage) error { return nil }

var errBoom = errors.New("boom")
