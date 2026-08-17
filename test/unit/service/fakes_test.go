// Package service_test provides hand-rolled fakes for every port
// InstanceService/TaskService/WorkflowClient/ArchiveGuard/UserTaskPauser/the
// internal-event reconcilers depend on — mirroring test/unit/handler/
// testhelper_test.go's own func-field-per-method convention (no mockgen at
// this layer, matching this repo's established style).
package service_test

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

// --- InstanceRepository ---

type fakeInstanceRepo struct {
	byID              map[uuid.UUID]*domain.Instance
	createErr         error
	getErr            error
	updateErr         error
	listErr           error
	countErr          error
	countResult       int64
	updateNodeErr     error
	nextCursor        *port.Cursor
	lastPageAfter     *port.Cursor
	listByTenantCalls int
}

func newFakeInstanceRepo(instances ...*domain.Instance) *fakeInstanceRepo {
	m := make(map[uuid.UUID]*domain.Instance, len(instances))
	for _, i := range instances {
		m[i.ID] = i
	}
	return &fakeInstanceRepo{byID: m}
}

func (r *fakeInstanceRepo) Create(_ context.Context, inst *domain.Instance) error {
	if r.createErr != nil {
		return r.createErr
	}
	if _, exists := r.byID[inst.ID]; exists {
		return domain.ErrAlreadyExists
	}
	for _, existing := range r.byID {
		if existing.TenantID == inst.TenantID && existing.BusinessKey == inst.BusinessKey &&
			existing.Status != domain.InstanceStatusCompleted && existing.Status != domain.InstanceStatusTerminated && existing.Status != domain.InstanceStatusFailed {
			return domain.ErrDuplicateBusinessKey
		}
	}
	inst.RecordVersion = 1
	inst.CreatedAt = time.Now().UTC()
	inst.UpdatedAt = inst.CreatedAt
	r.byID[inst.ID] = inst
	return nil
}

func (r *fakeInstanceRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.Instance, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	inst, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return inst, nil
}

func (r *fakeInstanceRepo) UpdateStatus(_ context.Context, _, id uuid.UUID, status domain.InstanceStatus, recordVersion int64) (*domain.Instance, error) {
	if r.updateErr != nil {
		return nil, r.updateErr
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
	if status == domain.InstanceStatusCompleted || status == domain.InstanceStatusTerminated || status == domain.InstanceStatusFailed {
		now := time.Now().UTC()
		inst.CompletedAt = &now
	}
	return inst, nil
}

func (r *fakeInstanceRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, page port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	if r.listErr != nil {
		return nil, nil, r.listErr
	}
	r.lastPageAfter = page.After
	r.listByTenantCalls++
	var out []*domain.Instance
	for _, inst := range r.byID {
		if inst.TenantID != tenantID {
			continue
		}
		if filter.Status != nil && inst.Status != *filter.Status {
			continue
		}
		if filter.WorkflowVersionID != nil && inst.WorkflowVersionID != *filter.WorkflowVersionID {
			continue
		}
		out = append(out, inst)
	}
	// nextCursor is consumed on its first use — a real second page would
	// return distinct rows and eventually a nil cursor; this fake has no
	// notion of "already returned" rows, so returning the same non-nil
	// cursor forever would spin allInstancesByStatus's pagination loop.
	cursor := r.nextCursor
	r.nextCursor = nil
	return out, cursor, nil
}

func (r *fakeInstanceRepo) UpdateCurrentNodeKeys(_ context.Context, _, id uuid.UUID, keys []string, recordVersion int64) (*domain.Instance, error) {
	if r.updateNodeErr != nil {
		return nil, r.updateNodeErr
	}
	inst, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	inst.CurrentNodeKeys = keys
	inst.RecordVersion++
	return inst, nil
}

func (r *fakeInstanceRepo) CountActiveByWorkflow(_ context.Context, _, _ uuid.UUID) (int64, error) {
	return r.countResult, r.countErr
}

func (r *fakeInstanceRepo) CountActiveByTaskQueue(_ context.Context, tenantID uuid.UUID, taskQueue string) (int64, error) {
	if r.countErr != nil {
		return 0, r.countErr
	}
	var count int64
	for _, inst := range r.byID {
		if inst.TenantID == tenantID && inst.TaskQueue == taskQueue &&
			(inst.Status == domain.InstanceStatusRunning || inst.Status == domain.InstanceStatusPaused || inst.Status == domain.InstanceStatusDegraded) {
			count++
		}
	}
	return count, nil
}

// --- TaskRepository ---

type fakeTaskRepo struct {
	byID            map[uuid.UUID]*domain.Task
	createErr       error
	updateStatusErr error
	listErr         error
}

func newFakeTaskRepo(tasks ...*domain.Task) *fakeTaskRepo {
	m := make(map[uuid.UUID]*domain.Task, len(tasks))
	for _, t := range tasks {
		m[t.ID] = t
	}
	return &fakeTaskRepo{byID: m}
}

func (r *fakeTaskRepo) Create(_ context.Context, task *domain.Task) error {
	if r.createErr != nil {
		return r.createErr
	}
	task.RecordVersion = 1
	r.byID[task.ID] = task
	return nil
}

func (r *fakeTaskRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.Task, error) {
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
	var out []*domain.Task
	for _, t := range r.byID {
		if t.WorkflowInstanceID == instanceID {
			out = append(out, t)
		}
	}
	return out, nil, nil
}

func (r *fakeTaskRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.TaskListFilter, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	if r.listErr != nil {
		return nil, nil, r.listErr
	}
	var out []*domain.Task
	for _, t := range r.byID {
		if t.TenantID != tenantID {
			continue
		}
		if filter.Status != nil && t.Status != *filter.Status {
			continue
		}
		out = append(out, t)
	}
	return out, nil, nil
}

func (r *fakeTaskRepo) GetByInstanceAndNode(_ context.Context, _, instanceID uuid.UUID, nodeKey string) (*domain.Task, error) {
	for _, t := range r.byID {
		if t.WorkflowInstanceID == instanceID && t.NodeKey == nodeKey {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

// --- TaskAssignmentRepository ---

type fakeAssignmentRepo struct {
	byID                         map[uuid.UUID]*domain.TaskAssignment
	createErr                    error
	vacateErr                    error
	vacateAllErr                 error
	listActiveByUserErr          error
	listActiveByTaskErr          error
	listActiveByUserPaginatedErr error
	setLeadErr                   error
}

func newFakeAssignmentRepo(assignments ...*domain.TaskAssignment) *fakeAssignmentRepo {
	m := make(map[uuid.UUID]*domain.TaskAssignment, len(assignments))
	for _, a := range assignments {
		m[a.ID] = a
	}
	return &fakeAssignmentRepo{byID: m}
}

func (r *fakeAssignmentRepo) Create(_ context.Context, a *domain.TaskAssignment) error {
	if r.createErr != nil {
		return r.createErr
	}
	a.IsActive = true
	r.byID[a.ID] = a
	return nil
}

func (r *fakeAssignmentRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.TaskAssignment, error) {
	a, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return a, nil
}

func (r *fakeAssignmentRepo) ListActiveByTask(_ context.Context, _, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if r.listActiveByTaskErr != nil {
		return nil, r.listActiveByTaskErr
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
	if r.listActiveByUserErr != nil {
		return nil, r.listActiveByUserErr
	}
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.UserID == userID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeAssignmentRepo) ListActiveByUserPaginated(_ context.Context, _, userID uuid.UUID, _ port.PageRequest) ([]port.ActiveUserTaskRow, *port.Cursor, error) {
	if r.listActiveByUserPaginatedErr != nil {
		return nil, nil, r.listActiveByUserPaginatedErr
	}
	var out []port.ActiveUserTaskRow
	for _, a := range r.byID {
		if a.UserID == userID && a.IsActive {
			out = append(out, port.ActiveUserTaskRow{TaskID: a.TaskID, UserID: a.UserID})
		}
	}
	return out, nil, nil
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

func (r *fakeAssignmentRepo) VacateAllActiveByUser(_ context.Context, _, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if r.vacateAllErr != nil {
		return nil, r.vacateAllErr
	}
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.UserID == userID && a.IsActive {
			a.IsActive = false
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeAssignmentRepo) Complete(_ context.Context, _, id uuid.UUID, resultJSON json.RawMessage, _ int64) (*domain.TaskAssignment, error) {
	a, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	a.IsActive = false
	a.ResultJSON = resultJSON
	return a, nil
}

func (r *fakeAssignmentRepo) SetLead(_ context.Context, _, _, id uuid.UUID, _ int64) (*domain.TaskAssignment, error) {
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

// --- AssigneeOverrideRepository ---

type fakeAssigneeOverrideRepo struct {
	created   []*domain.AssigneeOverride
	createErr error
	listErr   error
}

func (r *fakeAssigneeOverrideRepo) Create(_ context.Context, o *domain.AssigneeOverride) error {
	if r.createErr != nil {
		return r.createErr
	}
	o.CreatedAt = time.Now().UTC()
	r.created = append(r.created, o)
	return nil
}

func (r *fakeAssigneeOverrideRepo) ListByInstance(_ context.Context, _, _ uuid.UUID) ([]*domain.AssigneeOverride, error) {
	return r.created, r.listErr
}

// --- ActiveTaskQueueRepository ---

type fakeActiveTaskQueueRepo struct {
	byName        map[string]*domain.ActiveTaskQueue
	registerErr   error
	deregisterErr error
}

func newFakeActiveTaskQueueRepo() *fakeActiveTaskQueueRepo {
	return &fakeActiveTaskQueueRepo{byName: map[string]*domain.ActiveTaskQueue{}}
}

func (r *fakeActiveTaskQueueRepo) ListActive(_ context.Context) ([]*domain.ActiveTaskQueue, error) {
	var out []*domain.ActiveTaskQueue
	for _, q := range r.byName {
		out = append(out, q)
	}
	return out, nil
}

func (r *fakeActiveTaskQueueRepo) GetByQueueName(_ context.Context, queueName string) (*domain.ActiveTaskQueue, error) {
	q, ok := r.byName[queueName]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return q, nil
}

func (r *fakeActiveTaskQueueRepo) Register(_ context.Context, tenantID uuid.UUID, queueName string) (*domain.ActiveTaskQueue, error) {
	if r.registerErr != nil {
		return nil, r.registerErr
	}
	q := &domain.ActiveTaskQueue{ID: uuid.New(), TenantID: tenantID, QueueName: queueName, RegisteredAt: time.Now().UTC()}
	r.byName[queueName] = q
	return q, nil
}

func (r *fakeActiveTaskQueueRepo) Deregister(_ context.Context, queueName string) error {
	if r.deregisterErr != nil {
		return r.deregisterErr
	}
	delete(r.byName, queueName)
	return nil
}

// --- OutboxRepository ---

type fakeOutbox struct {
	enqueued   []events.Envelope[json.RawMessage]
	enqueueErr error
	records    []*domain.OutboxEventRecord
	listErr    error
}

func (o *fakeOutbox) Enqueue(_ context.Context, env events.Envelope[json.RawMessage]) error {
	if o.enqueueErr != nil {
		return o.enqueueErr
	}
	o.enqueued = append(o.enqueued, env)
	return nil
}

func (o *fakeOutbox) ListByInstance(_ context.Context, _, _ uuid.UUID, _ port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	return o.records, nil, o.listErr
}

func (o *fakeOutbox) ExistsForTask(_ context.Context, _ string, _ uuid.UUID) (bool, error) {
	return false, nil
}

// --- Transactor ---

// fakeTransactor just runs fn directly against the given ctx — these tests
// have no real transaction/connection to acquire (same pattern
// internal/adapter/outbound/temporal's own tests already use).
type fakeTransactor struct{}

func (fakeTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (fakeTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// --- TemporalClient ---

type fakeTemporalClient struct {
	startFunc     func(ctx context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error)
	signalFunc    func(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error
	terminateFunc func(ctx context.Context, temporalWorkflowID, reason string) error
	queryFunc     func(ctx context.Context, temporalWorkflowID, queryType string, out any) error

	signals   []signalCall
	terminate []terminateCall
}

type signalCall struct {
	TemporalWorkflowID string
	InstanceID         uuid.UUID
	SignalName         string
	Payload            any
}

type terminateCall struct {
	TemporalWorkflowID string
	Reason             string
}

func (c *fakeTemporalClient) terminateCalls() []terminateCall {
	return c.terminate
}

func (c *fakeTemporalClient) StartWorkflow(ctx context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	if c.startFunc != nil {
		return c.startFunc(ctx, in)
	}
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
}

func (c *fakeTemporalClient) SignalWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	c.signals = append(c.signals, signalCall{TemporalWorkflowID: temporalWorkflowID, InstanceID: instanceID, SignalName: signalName, Payload: payload})
	if c.signalFunc != nil {
		return c.signalFunc(ctx, temporalWorkflowID, instanceID, signalName, payload)
	}
	return nil
}

func (c *fakeTemporalClient) TerminateWorkflow(ctx context.Context, temporalWorkflowID, reason string) error {
	c.terminate = append(c.terminate, terminateCall{TemporalWorkflowID: temporalWorkflowID, Reason: reason})
	if c.terminateFunc != nil {
		return c.terminateFunc(ctx, temporalWorkflowID, reason)
	}
	return nil
}

func (c *fakeTemporalClient) QueryWorkflow(ctx context.Context, temporalWorkflowID, queryType string, out any) error {
	if c.queryFunc != nil {
		return c.queryFunc(ctx, temporalWorkflowID, queryType, out)
	}
	return nil
}

// --- DefinitionServiceClient ---

type fakeDefinitionClient struct {
	resp *port.CompiledWorkflow
	err  error
}

func (c *fakeDefinitionClient) GetCompiledWorkflow(_ context.Context, _, _ uuid.UUID) (*port.CompiledWorkflow, error) {
	return c.resp, c.err
}

// --- EligibilityChecker ---

type fakeEligibilityChecker struct {
	check      func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error)
	batchCheck func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]bool, error)
}

func (f *fakeEligibilityChecker) CheckEligibility(ctx context.Context, newUserID, departmentID uuid.UUID, requiredLevel string, actorID uuid.UUID) (bool, error) {
	if f.check != nil {
		return f.check(ctx, newUserID, departmentID, requiredLevel, actorID)
	}
	return true, nil
}

func (f *fakeEligibilityChecker) CheckEligibilityBatch(ctx context.Context, requests []port.EligibilityCheckRequest, actorID uuid.UUID) ([]bool, error) {
	if f.batchCheck != nil {
		return f.batchCheck(ctx, requests, actorID)
	}
	results := make([]bool, len(requests))
	for i := range results {
		results[i] = true
	}
	return results, nil
}

// --- CacheStore ---

type fakeCacheStore struct {
	values   map[string]string
	getErr   error
	setErr   error
	setCalls []string
}

func newFakeCacheStore() *fakeCacheStore {
	return &fakeCacheStore{values: map[string]string{}}
}

func (c *fakeCacheStore) Get(_ context.Context, key string) (string, error) {
	if c.getErr != nil {
		return "", c.getErr
	}
	return c.values[key], nil
}

func (c *fakeCacheStore) Set(_ context.Context, key, value string, _ time.Duration) error {
	if c.setErr != nil {
		return c.setErr
	}
	c.setCalls = append(c.setCalls, key)
	c.values[key] = value
	return nil
}

func (c *fakeCacheStore) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

func (c *fakeCacheStore) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	if _, ok := c.values[key]; ok {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *fakeCacheStore) Ping(_ context.Context) error { return nil }

// --- Logger ---

// fakeLogger records every call — used to exercise each service's own
// "s.Log != nil" branch, which leaving Log unset (the noopLogger fallback
// path every other test in this package already exercises) never reaches.
type fakeLogger struct {
	warnCalls  []string
	errorCalls []string
}

func (l *fakeLogger) Debug(string, map[string]any) {}
func (l *fakeLogger) Info(string, map[string]any)  {}
func (l *fakeLogger) Warn(msg string, _ map[string]any) {
	l.warnCalls = append(l.warnCalls, msg)
}
func (l *fakeLogger) Error(msg string, _ map[string]any) {
	l.errorCalls = append(l.errorCalls, msg)
}

// --- IAMClient ---

type fakeIAMClient struct {
	status port.UserStatus
	err    error
}

func (c *fakeIAMClient) GetUserStatus(_ context.Context, _, _ uuid.UUID) (port.UserStatus, error) {
	return c.status, c.err
}

// --- EventValidator ---

// noopValidator always accepts — these tests assert on repo/outbox state and
// signal construction, not schema validation itself (already covered by
// test/unit/eventbus).
type noopValidator struct{}

func (noopValidator) Validate(context.Context, string, json.RawMessage) error { return nil }

// failingValidator always rejects — exercises BuildEnvelope's own error
// path from a caller's perspective without needing a real JSON Schema.
type failingValidator struct{}

var errValidationFailed = errors.New("validation failed")

func (failingValidator) Validate(context.Context, string, json.RawMessage) error {
	return errValidationFailed
}
