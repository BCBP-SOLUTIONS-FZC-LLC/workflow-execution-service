package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW11aCacheDown     = errors.New("w11a: cache unreachable")
	errW11aSignalRefused = errors.New("w11a: workflow engine refused the signal")
	errW11aPrincipalDown = errors.New("w11a: principal lookup failed")
)

var w11aActor = uuid.MustParse("66666666-6666-6666-6666-666666666666")

const w11aBarrierWait = 30 * time.Second

type w11aRead struct{ Tenant, ID uuid.UUID }

type w11aTasks struct {
	port.TaskRepository
	mu    sync.Mutex
	rows  []*domain.Task
	reads []w11aRead
}

func (r *w11aTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, w11aRead{tenantID, id})
	for _, t := range r.rows {
		if t.TenantID == tenantID && t.ID == id {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w11aInstances struct {
	port.InstanceRepository
	mu    sync.Mutex
	rows  []*domain.Instance
	reads []w11aRead
}

func (r *w11aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, w11aRead{tenantID, id})
	for _, i := range r.rows {
		if i.TenantID == tenantID && i.ID == id {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

// w11aAssignments keeps one active row per task and user, as the table does, and
// counts every insert attempt.
type w11aAssignments struct {
	port.TaskAssignmentRepository
	mu       sync.Mutex
	attempts []domain.TaskAssignment
	rows     []domain.TaskAssignment
}

func (r *w11aAssignments) Create(_ context.Context, a *domain.TaskAssignment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, *a)
	for _, existing := range r.rows {
		if existing.TaskID == a.TaskID && existing.UserID == a.UserID {
			return domain.ErrDuplicateActiveAssignment
		}
	}
	r.rows = append(r.rows, *a)
	return nil
}

func (r *w11aAssignments) counts() (attempts, rows int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts), len(r.rows)
}

type w11aSignal struct {
	WorkflowID string
	Instance   uuid.UUID
	Name       string
	Payload    any
}

// w11aTemporal records every signal; signalFn, when set, answers each one by its
// 1-based call number.
type w11aTemporal struct {
	port.TemporalClient
	mu       sync.Mutex
	signals  []w11aSignal
	signalFn func(ctx context.Context, call int) error
}

func (c *w11aTemporal) SignalWorkflow(ctx context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.mu.Lock()
	c.signals = append(c.signals, w11aSignal{workflowID, instanceID, name, payload})
	call, fn := len(c.signals), c.signalFn
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx, call)
	}
	return nil
}

func (c *w11aTemporal) sent() []w11aSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]w11aSignal(nil), c.signals...)
}

type w11aCacheOp struct {
	Op     string
	Key    string
	Value  string
	TTL    time.Duration
	CtxErr error
}

type w11aCache struct {
	mu       sync.Mutex
	values   map[string]string
	ops      []w11aCacheOp
	getErr   map[string]error
	setNXErr error
	setErr   error
}

func newW11aCache() *w11aCache {
	return &w11aCache{values: map[string]string{}, getErr: map[string]error{}}
}

func (c *w11aCache) Get(ctx context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ops = append(c.ops, w11aCacheOp{Op: "get", Key: key, CtxErr: ctx.Err()})
	if err := c.getErr[key]; err != nil {
		return "", err
	}
	return c.values[key], nil
}

func (c *w11aCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ops = append(c.ops, w11aCacheOp{Op: "set", Key: key, Value: value, TTL: ttl, CtxErr: ctx.Err()})
	if c.setErr != nil {
		return c.setErr
	}
	c.values[key] = value
	return nil
}

func (c *w11aCache) Del(ctx context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		c.ops = append(c.ops, w11aCacheOp{Op: "del", Key: k, CtxErr: ctx.Err()})
		delete(c.values, k)
	}
	return nil
}

func (c *w11aCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ops = append(c.ops, w11aCacheOp{Op: "setnx", Key: key, Value: value, TTL: ttl, CtxErr: ctx.Err()})
	if c.setNXErr != nil {
		return false, c.setNXErr
	}
	if _, held := c.values[key]; held {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *w11aCache) Ping(context.Context) error { return nil }

func (c *w11aCache) value(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.values[key]
	return v, ok
}

func (c *w11aCache) put(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = value
}

// expire drops a key the way its time to live running out would.
func (c *w11aCache) expire(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.values, key)
}

func (c *w11aCache) opsNamed(op, key string) []w11aCacheOp {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []w11aCacheOp
	for _, o := range c.ops {
		if o.Op == op && o.Key == key {
			out = append(out, o)
		}
	}
	return out
}

func (c *w11aCache) opCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ops)
}

type w11aPrincipals struct {
	mu      sync.Mutex
	id      uuid.UUID
	err     error
	tenants []uuid.UUID
}

func (p *w11aPrincipals) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenants = append(p.tenants, tenantID)
	return port.AutomationPrincipal{ID: p.id, Subject: "sub-" + p.id.String()}, p.err
}

type w11aLogEntry struct {
	Level  string
	Msg    string
	Fields map[string]any
}

type w11aLogger struct {
	mu      sync.Mutex
	entries []w11aLogEntry
}

func (l *w11aLogger) add(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, w11aLogEntry{level, msg, fields})
}

func (l *w11aLogger) Debug(msg string, f map[string]any) { l.add("debug", msg, f) }
func (l *w11aLogger) Info(msg string, f map[string]any)  { l.add("info", msg, f) }
func (l *w11aLogger) Warn(msg string, f map[string]any)  { l.add("warn", msg, f) }
func (l *w11aLogger) Error(msg string, f map[string]any) { l.add("error", msg, f) }

func (l *w11aLogger) at(level string) []w11aLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []w11aLogEntry
	for _, e := range l.entries {
		if e.Level == level {
			out = append(out, e)
		}
	}
	return out
}

// w11aConnector is a connector task service over recording fakes: a READY storage
// task at node dept-9/node-A, on a running instance, in one tenant.
type w11aConnector struct {
	svc         *service.ConnectorTaskService
	tenant      uuid.UUID
	task        *domain.Task
	inst        *domain.Instance
	tasks       *w11aTasks
	instances   *w11aInstances
	assignments *w11aAssignments
	temporal    *w11aTemporal
	cache       *w11aCache
	principals  *w11aPrincipals
	log         *w11aLogger
}

func w11aNewConnector(extras string) *w11aConnector {
	tenant := uuid.New()
	connectorType := "storage"
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenant, TemporalWorkflowID: "w11a:" + uuid.NewString(), Status: domain.InstanceStatusRunning}
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, NodeKey: "dept-9/node-A",
		Status: domain.TaskStatusReady, RecordVersion: 6, VisitCount: 4, ConnectorType: &connectorType,
		ExtrasJSON: json.RawMessage(extras),
	}
	h := &w11aConnector{
		tenant: tenant, task: task, inst: inst,
		tasks:       &w11aTasks{rows: []*domain.Task{task}},
		instances:   &w11aInstances{rows: []*domain.Instance{inst}},
		assignments: &w11aAssignments{},
		temporal:    &w11aTemporal{},
		cache:       newW11aCache(),
		principals:  &w11aPrincipals{id: w11aActor},
		log:         &w11aLogger{},
	}
	h.svc = &service.ConnectorTaskService{
		Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments, Temporal: h.temporal,
		Cache: h.cache, Principals: h.principals, Log: h.log,
	}
	return h
}

func (h *w11aConnector) complete(output map[string]any) error {
	return h.svc.Complete(context.Background(), h.tenant, h.task.ID, output)
}

func (h *w11aConnector) fail(class string, output map[string]any) error {
	return h.svc.Fail(context.Background(), h.tenant, h.task.ID, class, output)
}

func (h *w11aConnector) key(outcome string) string {
	return "connector-signal:" + outcome + ":" + h.task.ID.String()
}

type w11aTransitionWire struct {
	DeptID        string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
	VisitCount    int64
}

type w11aFailWire struct {
	DeptID        string `json:"dept_id"`
	NodeID        string `json:"node_id"`
	ConnectorType string `json:"connector_type"`
	ErrorClass    string `json:"error_class"`
	RecordVersion int64  `json:"record_version"`
	VisitCount    int64  `json:"visit_count"`
}

func w11aDecode[T any](t *testing.T, payload any) T {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

func (h *w11aConnector) requireActorHeld(t *testing.T) {
	t.Helper()
	attempts, rows := h.assignments.counts()
	assert.Equal(t, 1, rows, "the principal holds one assignment on the task")
	require.NotZero(t, attempts)
	h.assignments.mu.Lock()
	defer h.assignments.mu.Unlock()
	assert.Equal(t, h.tenant, h.assignments.rows[0].TenantID)
	assert.Equal(t, h.task.ID, h.assignments.rows[0].TaskID)
	assert.Equal(t, w11aActor, h.assignments.rows[0].UserID)
}

func TestConnectorTaskService_Complete_SignalCarriesNodeSplitActorAndTaskPosition(t *testing.T) {
	t.Parallel()

	h := w11aNewConnector(`{}`)

	require.NoError(t, h.complete(map[string]any{"k": "v"}))

	sent := h.temporal.sent()
	require.Len(t, sent, 1)
	assert.Equal(t, "stage-transition", sent[0].Name)
	assert.Equal(t, h.inst.TemporalWorkflowID, sent[0].WorkflowID)
	assert.Equal(t, h.inst.ID, sent[0].Instance)
	wire := w11aDecode[w11aTransitionWire](t, sent[0].Payload)
	assert.Equal(t, "dept-9", wire.DeptID, "the part of the node key before the slash")
	assert.Equal(t, "node-A", wire.NodeID, "the part after it")
	assert.Equal(t, w11aActor.String(), wire.UserID, "the automation principal, never a caller-named user")
	assert.Equal(t, int64(6), wire.RecordVersion)
	assert.Equal(t, int64(4), wire.VisitCount)
	assert.JSONEq(t, `{"k":"v"}`, wire.ResultJSON)
	assert.Equal(t, []uuid.UUID{h.tenant}, h.principals.tenants)
	h.requireActorHeld(t)
}

func TestConnectorTaskService_ReservationUnavailable_SignalsAnyway(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		outcome  string
		nilCache bool
		wantWarn bool
	}{
		{"complete without a cache", "complete", true, false},
		{"complete with a failing reservation", "complete", false, true},
		{"fail without a cache", "fail", true, false},
		{"fail with a failing reservation", "fail", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{}`)
			if tc.nilCache {
				h.svc.Cache = nil
			} else {
				h.cache.setNXErr = errW11aCacheDown
			}

			var err error
			if tc.outcome == "complete" {
				err = h.complete(map[string]any{})
			} else {
				err = h.fail("upstream_error", nil)
			}

			require.NoError(t, err, "a cache outage never blocks the callback")
			sent := h.temporal.sent()
			require.Len(t, sent, 1)
			wantSignal := map[string]string{"complete": "stage-transition", "fail": "stage-fail"}[tc.outcome]
			assert.Equal(t, wantSignal, sent[0].Name)
			h.requireActorHeld(t)
			warns := w11aCauseWarns(h.log)
			if !tc.wantWarn {
				assert.Empty(t, warns, "no cache, nothing to warn about")
				return
			}
			require.Len(t, h.cache.opsNamed("setnx", h.key(tc.outcome)), 1, "the reservation was attempted")
			require.Len(t, warns, 1)
			assert.Equal(t, h.task.ID, warns[0].Fields["task_id"])
			assert.Equal(t, errW11aCacheDown.Error(), warns[0].Fields["error"])
		})
	}
}

// w11aCauseWarns are the warnings that report a failure, which a fail callback's
// own "connector task failed" notice is not.
func w11aCauseWarns(l *w11aLogger) []w11aLogEntry {
	var out []w11aLogEntry
	for _, e := range l.at("warn") {
		if _, ok := e.Fields["error"]; ok {
			out = append(out, e)
		}
	}
	return out
}

func TestConnectorTaskService_Complete_DeliveredMarkFails_AnswersAndLaterRetryIsAcknowledgedByTaskStatus(t *testing.T) {
	t.Parallel()

	h := w11aNewConnector(`{}`)
	h.cache.setErr = errW11aCacheDown
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h.temporal.signalFn = func(context.Context, int) error {
		cancel()
		return nil
	}
	key := h.key("complete")

	require.NoError(t, h.svc.Complete(ctx, h.tenant, h.task.ID, map[string]any{}), "the signal went out, so the callback is answered")

	require.Len(t, h.temporal.sent(), 1)
	sets := h.cache.opsNamed("set", key)
	require.Len(t, sets, 1)
	assert.Equal(t, "delivered", sets[0].Value)
	assert.Equal(t, 5*time.Minute, sets[0].TTL)
	assert.NoError(t, sets[0].CtxErr, "the mark is written on a context the caller's give-up does not cancel")
	warns := h.log.at("warn")
	require.Len(t, warns, 1)
	assert.Equal(t, h.task.ID, warns[0].Fields["task_id"])
	assert.Equal(t, errW11aCacheDown.Error(), warns[0].Fields["error"])
	state, held := h.cache.value(key)
	require.True(t, held)
	assert.Equal(t, "reserved", state, "only the short reservation stands")
	assert.Equal(t, 30*time.Second, h.cache.opsNamed("setnx", key)[0].TTL)

	require.ErrorIs(t, h.complete(map[string]any{}), port.ErrCallbackInFlight, "until it expires the retry is asked to come back")
	assert.Len(t, h.temporal.sent(), 1)

	// The signal made the workflow complete the task, so by the time the reservation expires the
	// task is no longer open and the status guard acknowledges the retry without a signal.
	h.task.Status = domain.TaskStatusCompleted
	h.cache.expire(key)
	h.cache.setErr = nil
	require.NoError(t, h.complete(map[string]any{}))
	assert.Len(t, h.temporal.sent(), 1, "the retry after expiry is acknowledged, not signalled again")
	attempts, _ := h.assignments.counts()
	assert.Equal(t, 1, attempts, "and writes no assignment")
}

func TestConnectorTaskService_Complete_OtherOutcomeReadFailsOrIsAbsent_SignalsAnyway(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		readErr  error
		wantWarn bool
	}{
		{"the read fails", errW11aCacheDown, true},
		{"the key is absent", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{}`)
			if tc.readErr != nil {
				h.cache.getErr[h.key("fail")] = tc.readErr
			}

			require.NoError(t, h.complete(map[string]any{}))

			require.Len(t, h.temporal.sent(), 1)
			assert.Len(t, h.cache.opsNamed("get", h.key("fail")), 2, "the other outcome's reservation was read before the principal lookup and again when reserving")
			assert.Len(t, h.cache.opsNamed("get", h.key("complete")), 1, "the callback's own reservation was read once, before the principal lookup")
			state, _ := h.cache.value(h.key("complete"))
			assert.Equal(t, "delivered", state)
			warns := h.log.at("warn")
			if !tc.wantWarn {
				assert.Empty(t, warns, "a missing key is an absent reservation, not an error")
				return
			}
			require.Len(t, warns, 2, "one warning per failed read")
			for _, w := range warns {
				assert.Equal(t, h.task.ID, w.Fields["task_id"])
				assert.Equal(t, tc.readErr.Error(), w.Fields["error"])
			}
		})
	}
}

func w11aWaitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(w11aBarrierWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestConnectorTaskService_Complete_RetryDuringInFlightSignal_AsksToWaitThenIsAcknowledgedOrResignals(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		winnerErr   error
		wantSignals int
	}{
		{"the first attempt delivers", nil, 1},
		{"the first attempt's signal fails and releases the reservation", errW11aSignalRefused, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{}`)
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			h.temporal.signalFn = func(_ context.Context, call int) error {
				if call > 1 {
					return nil
				}
				entered <- struct{}{}
				select {
				case <-release:
				case <-time.After(w11aBarrierWait):
					return errors.New("w11a: the barrier was never released")
				}
				return tc.winnerErr
			}

			winner := make(chan error, 1)
			go func() { winner <- h.complete(map[string]any{}) }()
			w11aWaitFor(t, entered, "the first attempt to reach the signal")

			require.ErrorIs(t, h.complete(map[string]any{}), port.ErrCallbackInFlight,
				"the second attempt finds the reservation, so it neither inserts nor signals")
			attempts, _ := h.assignments.counts()
			assert.Equal(t, 1, attempts, "only the first attempt inserted an assignment")
			assert.Len(t, h.temporal.sent(), 1)

			once.Do(func() { close(release) })
			select {
			case err := <-winner:
				if tc.winnerErr != nil {
					require.ErrorIs(t, err, tc.winnerErr)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(w11aBarrierWait):
				t.Fatal("timed out waiting for the first attempt to finish")
			}

			require.NoError(t, h.complete(map[string]any{}), "the retry is acknowledged or signals, never fails")
			assert.Len(t, h.temporal.sent(), tc.wantSignals)
			_, rows := h.assignments.counts()
			assert.Equal(t, 1, rows, "one assignment row however many attempts")
			state, _ := h.cache.value(h.key("complete"))
			assert.Equal(t, "delivered", state)
		})
	}
}

func TestConnectorTaskService_Complete_SignalFails_KeepsCauseReleasesKeyAndKeepsAssignment(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cause error
	}{
		{"workflow engine unavailable", fmt.Errorf("w11a: dial: %w", port.ErrTemporalUnavailable)},
		{"signal timed out", fmt.Errorf("w11a: signal: %w", context.DeadlineExceeded)},
		{"any other failure", errW11aSignalRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{}`)
			h.temporal.signalFn = func(context.Context, int) error { return tc.cause }
			key := h.key("complete")

			err := h.complete(map[string]any{})

			require.ErrorIs(t, err, tc.cause, "the cause reaches the handler, which maps it to a status")
			_, held := h.cache.value(key)
			assert.False(t, held, "the key is released, not left for its time to live")
			assert.Len(t, h.cache.opsNamed("del", key), 1)
			h.requireActorHeld(t)

			h.temporal.signalFn = nil
			require.NoError(t, h.complete(map[string]any{}))
			assert.Len(t, h.temporal.sent(), 2, "the retry signals again")
			state, _ := h.cache.value(key)
			assert.Equal(t, "delivered", state)
		})
	}
}

func TestConnectorTaskService_Complete_UnreadableMappingAndOtherOutcomeResolved_NoSignal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		otherSeen string
		wantErr   error
	}{
		{"the other outcome was delivered", "delivered", nil},
		{"the other outcome is still in flight", "reserved", port.ErrCallbackInFlight},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{not json`)
			h.cache.put(h.key("complete"), tc.otherSeen)

			err := h.complete(map[string]any{"x": 1})

			if tc.wantErr == nil {
				require.NoError(t, err, "acknowledged: the task already has its outcome")
			} else {
				require.ErrorIs(t, err, tc.wantErr, "answered with a retry request")
			}
			assert.Empty(t, h.temporal.sent())
			attempts, _ := h.assignments.counts()
			assert.Zero(t, attempts, "no assignment is written for a callback that does not signal")
			_, held := h.cache.value(h.key("fail"))
			assert.False(t, held, "the fail reservation taken for the check is given back")
			state, _ := h.cache.value(h.key("complete"))
			assert.Equal(t, tc.otherSeen, state, "the other outcome's record is untouched")
			assert.Len(t, h.cache.opsNamed("get", h.key("complete")), 1)
		})
	}
}

func TestConnectorTaskService_Complete_UnreadableMappingAndFailSignalFails_ReleasesFailKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cause error
	}{
		{"workflow engine unavailable", fmt.Errorf("w11a: dial: %w", port.ErrTemporalUnavailable)},
		{"any other failure", errW11aSignalRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{not json`)
			h.temporal.signalFn = func(context.Context, int) error { return tc.cause }
			key := h.key("fail")

			err := h.complete(map[string]any{"x": 1})

			require.ErrorIs(t, err, tc.cause)
			sent := h.temporal.sent()
			require.Len(t, sent, 1)
			assert.Equal(t, "stage-fail", sent[0].Name)
			assert.Equal(t, "output_mapping_invalid", w11aDecode[w11aFailWire](t, sent[0].Payload).ErrorClass)
			_, held := h.cache.value(key)
			assert.False(t, held, "the fail reservation is released")
			assert.Len(t, h.cache.opsNamed("del", key), 1)
			assert.Empty(t, h.cache.opsNamed("setnx", h.key("complete")), "the complete outcome never reserved anything")
			h.requireActorHeld(t)

			h.temporal.signalFn = nil
			require.NoError(t, h.complete(map[string]any{"x": 1}))
			require.Len(t, h.temporal.sent(), 2, "the retry signals the failure again")
			assert.Equal(t, "stage-fail", h.temporal.sent()[1].Name)
		})
	}
}

func TestConnectorTaskService_Callback_TaskOfAnotherTenantOrUnknown_NotFoundAndNothingElseTouched(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"complete", "fail"} {
		for _, scenario := range []string{"unknown task id", "task of another tenant"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				t.Parallel()
				h := w11aNewConnector(`{}`)
				callTenant, callTask := h.tenant, uuid.New()
				if scenario == "task of another tenant" {
					callTenant, callTask = uuid.New(), h.task.ID
				}

				var err error
				if kind == "complete" {
					err = h.svc.Complete(context.Background(), callTenant, callTask, map[string]any{})
				} else {
					err = h.svc.Fail(context.Background(), callTenant, callTask, "upstream_error", nil)
				}

				require.ErrorIs(t, err, port.ErrTaskNotFound)
				assert.Equal(t, []w11aRead{{callTenant, callTask}}, h.tasks.reads, "the read was scoped to the caller's tenant")
				assert.Empty(t, h.instances.reads)
				assert.Empty(t, h.principals.tenants)
				assert.Zero(t, h.cache.opCount())
				assert.Empty(t, h.temporal.sent())
				attempts, _ := h.assignments.counts()
				assert.Zero(t, attempts)
			})
		}
	}
}

func TestConnectorTaskService_Fail_SignalCarriesNodeSplitClassAndTaskPosition(t *testing.T) {
	t.Parallel()

	h := w11aNewConnector(`{}`)

	require.NoError(t, h.fail("upstream_timeout", map[string]any{"status": 504}))

	sent := h.temporal.sent()
	require.Len(t, sent, 1)
	assert.Equal(t, "stage-fail", sent[0].Name)
	assert.Equal(t, h.inst.TemporalWorkflowID, sent[0].WorkflowID)
	assert.Equal(t, h.inst.ID, sent[0].Instance)
	assert.Equal(t, w11aFailWire{
		DeptID: "dept-9", NodeID: "node-A", ConnectorType: "storage", ErrorClass: "upstream_timeout",
		RecordVersion: 6, VisitCount: 4,
	}, w11aDecode[w11aFailWire](t, sent[0].Payload))
	h.requireActorHeld(t)
	state, _ := h.cache.value(h.key("fail"))
	assert.Equal(t, "delivered", state)
}

func TestConnectorTaskService_Fail_LogsOnlySortedOutputKeys(t *testing.T) {
	t.Parallel()

	h := w11aNewConnector(`{}`)
	output := map[string]any{
		"status": 502, "body": "w11a-secret-body", "zeta": 1, "alpha": 2, "mid": 3, "headers": "w11a-secret-header",
	}

	require.NoError(t, h.fail("upstream_error", output))

	warns := h.log.at("warn")
	require.Len(t, warns, 1)
	assert.Equal(t, []string{"alpha", "body", "headers", "mid", "status", "zeta"}, warns[0].Fields["output_keys"])
	assert.NotContains(t, warns[0].Fields, "output")
	for _, e := range h.log.entries {
		assert.NotContains(t, fmt.Sprint(e.Fields), "w11a-secret", "no output value reaches a log line")
	}
	sent := h.temporal.sent()
	require.Len(t, sent, 1)
	b, err := json.Marshal(sent[0].Payload)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "w11a-secret", "the output is not part of the signal")
	assert.NotContains(t, string(b), "alpha")
}

func TestConnectorTaskService_Fail_PrincipalUnavailable_ReservesNothingAndChangesNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{"the lookup fails", errW11aPrincipalDown},
		{"the tenant has no principal", port.ErrNoAutomationPrincipal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{}`)
			h.principals.err = tc.err

			err := h.fail("upstream_error", nil)

			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, []uuid.UUID{h.tenant}, h.principals.tenants, "the lookup was for the task's tenant")
			assert.Empty(t, h.cache.opsNamed("setnx", h.key("fail")), "no reservation is taken before the principal is known")
			assert.Empty(t, h.cache.values, "and nothing is left in the cache")
			attempts, _ := h.assignments.counts()
			assert.Zero(t, attempts)
			assert.Empty(t, h.temporal.sent())
		})
	}
}

func TestConnectorTaskService_Fail_OutputAbsentOrEmpty_LogsNoKeysAndSignals(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		output map[string]any
	}{
		{"nil output", nil},
		{"empty output", map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w11aNewConnector(`{}`)

			require.NoError(t, h.fail("upstream_error", tc.output))

			warns := h.log.at("warn")
			require.Len(t, warns, 1)
			keys, ok := warns[0].Fields["output_keys"].([]string)
			require.True(t, ok, "output_keys is a list of key names")
			assert.Empty(t, keys)
			sent := h.temporal.sent()
			require.Len(t, sent, 1)
			assert.Equal(t, "stage-fail", sent[0].Name)
			assert.Equal(t, "upstream_error", w11aDecode[w11aFailWire](t, sent[0].Payload).ErrorClass)
		})
	}
}
