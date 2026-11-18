package service_test

import (
	"context"
	"errors"
	"strings"
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
	errW12bConnCacheDown  = errors.New("cache unreachable")
	errW12bConnSignalDown = errors.New("temporal refused the signal")
)

type w12bConnIDCall struct{ tenantID, id uuid.UUID }

type w12bConnTasks struct {
	port.TaskRepository
	task  *domain.Task
	reads []w12bConnIDCall
}

func (r *w12bConnTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w12bConnIDCall{tenantID, id})
	if tenantID != r.task.TenantID || id != r.task.ID {
		return nil, domain.ErrNotFound
	}
	return r.task, nil
}

type w12bConnInstances struct {
	port.InstanceRepository
	inst  *domain.Instance
	reads []w12bConnIDCall
}

func (r *w12bConnInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w12bConnIDCall{tenantID, id})
	if tenantID != r.inst.TenantID || id != r.inst.ID {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type w12bConnAssignments struct {
	port.TaskAssignmentRepository
	created []domain.TaskAssignment
}

func (r *w12bConnAssignments) Create(_ context.Context, a *domain.TaskAssignment) error {
	r.created = append(r.created, *a)
	return nil
}

type w12bConnSignal struct {
	temporalWorkflowID string
	instanceID         uuid.UUID
	name               string
}

type w12bConnTemporal struct {
	port.TemporalClient
	signals []w12bConnSignal
	err     error
}

func (c *w12bConnTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, name string, _ any) error {
	if c.err != nil {
		return c.err
	}
	c.signals = append(c.signals, w12bConnSignal{temporalWorkflowID, instanceID, name})
	return nil
}

type w12bConnPrincipals struct {
	id      uuid.UUID
	tenants []uuid.UUID
}

func (p *w12bConnPrincipals) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	p.tenants = append(p.tenants, tenantID)
	return port.AutomationPrincipal{ID: p.id}, nil
}

type w12bConnCacheOp struct {
	op    string
	key   string
	value string
	ttl   time.Duration
}

// w12bConnCache is a cache that remembers every call with its key, value and TTL, and fails the calls it is told to.
type w12bConnCache struct {
	values  map[string]string
	ops     []w12bConnCacheOp
	getErrs map[string]error
	setErr  error
	delErr  error
}

func newW12bConnCache() *w12bConnCache {
	return &w12bConnCache{values: map[string]string{}, getErrs: map[string]error{}}
}

func (c *w12bConnCache) Get(_ context.Context, key string) (string, error) {
	c.ops = append(c.ops, w12bConnCacheOp{op: "get", key: key})
	if err := c.getErrs[key]; err != nil {
		return "", err
	}
	return c.values[key], nil
}

func (c *w12bConnCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.ops = append(c.ops, w12bConnCacheOp{op: "set", key: key, value: value, ttl: ttl})
	if c.setErr != nil {
		return c.setErr
	}
	c.values[key] = value
	return nil
}

func (c *w12bConnCache) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		c.ops = append(c.ops, w12bConnCacheOp{op: "del", key: k})
	}
	if c.delErr != nil {
		return c.delErr
	}
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

func (c *w12bConnCache) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.ops = append(c.ops, w12bConnCacheOp{op: "setnx", key: key, value: value, ttl: ttl})
	if _, held := c.values[key]; held {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *w12bConnCache) Ping(context.Context) error { return nil }

// expire drops a key the way its TTL passing would.
func (c *w12bConnCache) expire(key string) { delete(c.values, key) }

func w12bOtherOutcome(outcome string) string {
	if outcome == "complete" {
		return "fail"
	}
	return "complete"
}

type w12bConnLogLine struct {
	msg    string
	fields map[string]any
}

type w12bConnLog struct{ all []w12bConnLogLine }

// dedupWarns are the warnings about the signal dedup state; the fail callback also logs its own failure notice.
func (l *w12bConnLog) dedupWarns() []w12bConnLogLine {
	var out []w12bConnLogLine
	for _, line := range l.all {
		if strings.Contains(line.msg, "signal dedup") {
			out = append(out, line)
		}
	}
	return out
}

func (l *w12bConnLog) Debug(string, map[string]any) {}
func (l *w12bConnLog) Info(string, map[string]any)  {}
func (l *w12bConnLog) Warn(msg string, f map[string]any) {
	l.all = append(l.all, w12bConnLogLine{msg, f})
}
func (l *w12bConnLog) Error(string, map[string]any) {}

type w12bConn struct {
	svc         *service.ConnectorTaskService
	task        *domain.Task
	inst        *domain.Instance
	tasks       *w12bConnTasks
	instances   *w12bConnInstances
	assignments *w12bConnAssignments
	temporal    *w12bConnTemporal
	principals  *w12bConnPrincipals
	cache       *w12bConnCache
	log         *w12bConnLog
}

func w12bNewConn() *w12bConn {
	connectorType := "storage"
	h := &w12bConn{
		task: &domain.Task{
			ID: uuid.New(), TenantID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "dept-1/nodeA",
			Status: domain.TaskStatusReady, RecordVersion: 3, ConnectorType: &connectorType,
		},
		assignments: &w12bConnAssignments{},
		temporal:    &w12bConnTemporal{},
		principals:  &w12bConnPrincipals{id: uuid.New()},
		cache:       newW12bConnCache(),
		log:         &w12bConnLog{},
	}
	h.inst = &domain.Instance{ID: h.task.WorkflowInstanceID, TenantID: h.task.TenantID, TemporalWorkflowID: "tenant:subject:business-key", Status: domain.InstanceStatusRunning}
	h.tasks = &w12bConnTasks{task: h.task}
	h.instances = &w12bConnInstances{inst: h.inst}
	h.svc = &service.ConnectorTaskService{
		Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments, Temporal: h.temporal,
		Cache: h.cache, Principals: h.principals, Log: h.log,
	}
	return h
}

func (h *w12bConn) key(outcome string) string {
	return "connector-signal:" + outcome + ":" + h.task.ID.String()
}

// call sends the callback of the given outcome.
func (h *w12bConn) call(outcome string) error {
	if outcome == "complete" {
		return h.svc.Complete(context.Background(), h.task.TenantID, h.task.ID, map[string]any{})
	}
	return h.svc.Fail(context.Background(), h.task.TenantID, h.task.ID, "upstream_error", nil)
}

func (h *w12bConn) signalName(outcome string) string {
	if outcome == "complete" {
		return "stage-transition"
	}
	return "stage-fail"
}

func (h *w12bConn) requireSignals(t *testing.T, outcome string, n int) {
	t.Helper()
	require.Len(t, h.temporal.signals, n)
	for _, s := range h.temporal.signals {
		assert.Equal(t, w12bConnSignal{h.inst.TemporalWorkflowID, h.inst.ID, h.signalName(outcome)}, s)
	}
}

func TestConnectorTaskService_TheDeliveredMarkLivesFiveMinutesAndACallbackAfterItExpiresSignalsAgain(t *testing.T) {
	for _, outcome := range []string{"complete", "fail"} {
		t.Run(outcome, func(t *testing.T) {
			h := w12bNewConn()
			key := h.key(outcome)

			require.NoError(t, h.call(outcome))

			h.requireSignals(t, outcome, 1)
			assert.Equal(t, []w12bConnCacheOp{
				{op: "get", key: key},
				{op: "get", key: h.key(w12bOtherOutcome(outcome))},
				{op: "setnx", key: key, value: "reserved", ttl: 30 * time.Second},
				{op: "get", key: h.key(w12bOtherOutcome(outcome))},
				{op: "set", key: key, value: "delivered", ttl: 5 * time.Minute},
			}, h.cache.ops, "the reservation lives 30 seconds and the delivered mark 5 minutes")
			assert.Equal(t, []w12bConnIDCall{{h.task.TenantID, h.task.ID}}, h.tasks.reads)
			assert.Equal(t, []w12bConnIDCall{{h.task.TenantID, h.inst.ID}}, h.instances.reads)
			assert.Equal(t, []uuid.UUID{h.task.TenantID}, h.principals.tenants)

			require.NoError(t, h.call(outcome), "a repeat inside the 5 minutes is acknowledged")
			h.requireSignals(t, outcome, 1)

			h.cache.expire(key)
			require.NoError(t, h.call(outcome))

			h.requireSignals(t, outcome, 2)
			assert.Equal(t, "delivered", h.cache.values[key], "the second signal is marked delivered again")
			assert.Len(t, h.assignments.created, 2, "each acted-on callback records the automation principal's assignment, the acknowledged repeat does not")
		})
	}
}

func TestConnectorTaskService_AnUnreadableSignalStateFailsOpenWithAWarning(t *testing.T) {
	cases := []struct {
		name      string
		outcome   string
		readFails bool
	}{
		{"complete with the other outcome's state unreadable", "complete", true},
		{"fail with the other outcome's state unreadable", "fail", true},
		{"complete with the other outcome's state readable and absent", "complete", false},
		{"fail with the other outcome's state readable and absent", "fail", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w12bNewConn()
			other := w12bOtherOutcome(tc.outcome)
			if tc.readFails {
				h.cache.getErrs[h.key(other)] = errW12bConnCacheDown
			}

			require.NoError(t, h.call(tc.outcome))

			h.requireSignals(t, tc.outcome, 1)
			assert.Equal(t, "delivered", h.cache.values[h.key(tc.outcome)])
			reads := map[string]int{}
			for _, o := range h.cache.ops {
				if o.op == "get" {
					reads[o.key]++
				}
			}
			assert.Equal(t, map[string]int{h.key(tc.outcome): 1, h.key(other): 2}, reads,
				"each state is read before the principal lookup; the other outcome's is read again when reserving")
			if !tc.readFails {
				assert.Empty(t, h.log.dedupWarns())
				return
			}
			require.Len(t, h.log.dedupWarns(), 2, "one warning per failed read")
			for _, w := range h.log.dedupWarns() {
				assert.Equal(t, h.task.ID, w.fields["task_id"])
				assert.Equal(t, errW12bConnCacheDown.Error(), w.fields["error"])
			}
		})
	}
}

func TestConnectorTaskService_AFailedDeliveredWriteOnlyLogsAndTheReservationKeepsRetriesWaiting(t *testing.T) {
	for _, outcome := range []string{"complete", "fail"} {
		t.Run(outcome, func(t *testing.T) {
			h := w12bNewConn()
			h.cache.setErr = errW12bConnCacheDown
			key := h.key(outcome)

			require.NoError(t, h.call(outcome), "the signal is out, so the callback is answered as done")

			h.requireSignals(t, outcome, 1)
			require.Len(t, h.log.dedupWarns(), 1)
			assert.Equal(t, h.task.ID, h.log.dedupWarns()[0].fields["task_id"])
			assert.Equal(t, errW12bConnCacheDown.Error(), h.log.dedupWarns()[0].fields["error"])
			assert.Equal(t, "reserved", h.cache.values[key], "the mark that failed to write leaves the reservation standing")

			require.ErrorIs(t, h.call(outcome), port.ErrCallbackInFlight, "a retry is told to come back, never acknowledged as delivered")
			h.requireSignals(t, outcome, 1)
		})
	}
}

func TestConnectorTaskService_AFailedSignalReleasesTheReservationAndAFailedReleaseOnlyLogs(t *testing.T) {
	cases := []struct {
		name       string
		outcome    string
		releaseErr error
	}{
		{"complete, release works", "complete", nil},
		{"fail, release works", "fail", nil},
		{"complete, release fails", "complete", errW12bConnCacheDown},
		{"fail, release fails", "fail", errW12bConnCacheDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w12bNewConn()
			h.temporal.err = errW12bConnSignalDown
			h.cache.delErr = tc.releaseErr
			key := h.key(tc.outcome)

			err := h.call(tc.outcome)

			require.ErrorIs(t, err, errW12bConnSignalDown, "the callback answers with the signal failure")
			assert.Empty(t, h.temporal.signals)
			var deletes []w12bConnCacheOp
			for _, o := range h.cache.ops {
				if o.op == "del" {
					deletes = append(deletes, o)
				}
			}
			assert.Equal(t, []w12bConnCacheOp{{op: "del", key: key}}, deletes, "exactly the callback's own reservation is released")
			assert.Empty(t, h.cache.opsOnSet(), "no delivered mark is written for a signal that did not go out")
			if tc.releaseErr == nil {
				assert.NotContains(t, h.cache.values, key, "the key is free at once")
				assert.Empty(t, h.log.dedupWarns())
				return
			}
			assert.Equal(t, "reserved", h.cache.values[key], "a failed release leaves the reservation to expire")
			require.Len(t, h.log.dedupWarns(), 1)
			assert.Equal(t, h.task.ID, h.log.dedupWarns()[0].fields["task_id"])
			assert.Equal(t, errW12bConnCacheDown.Error(), h.log.dedupWarns()[0].fields["error"])
		})
	}
}

func (c *w12bConnCache) opsOnSet() []w12bConnCacheOp {
	var out []w12bConnCacheOp
	for _, o := range c.ops {
		if o.op == "set" {
			out = append(out, o)
		}
	}
	return out
}
