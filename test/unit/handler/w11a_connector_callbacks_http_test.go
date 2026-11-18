package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW11aSignalRefused = errors.New("w11a: workflow engine refused the signal")

var w11aActor = uuid.MustParse("77777777-7777-7777-7777-777777777777")

type w11aRead struct{ Tenant, ID uuid.UUID }

type w11aTasks struct {
	port.TaskRepository
	rows  []*domain.Task
	reads []w11aRead
}

func (r *w11aTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
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
	rows []*domain.Instance
}

func (r *w11aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	for _, i := range r.rows {
		if i.TenantID == tenantID && i.ID == id {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w11aAssignments struct {
	port.TaskAssignmentRepository
	attempts []domain.TaskAssignment
	rows     []domain.TaskAssignment
}

func (r *w11aAssignments) Create(_ context.Context, a *domain.TaskAssignment) error {
	r.attempts = append(r.attempts, *a)
	for _, existing := range r.rows {
		if existing.TaskID == a.TaskID && existing.UserID == a.UserID {
			return domain.ErrDuplicateActiveAssignment
		}
	}
	r.rows = append(r.rows, *a)
	return nil
}

type w11aSignal struct {
	WorkflowID string
	Name       string
	Payload    any
}

type w11aTemporal struct {
	port.TemporalClient
	signals []w11aSignal
	err     error
}

func (c *w11aTemporal) SignalWorkflow(_ context.Context, workflowID string, _ uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w11aSignal{workflowID, name, payload})
	return c.err
}

type w11aCacheOp struct{ Op, Key string }

type w11aCache struct {
	values map[string]string
	ops    []w11aCacheOp
}

func (c *w11aCache) Get(_ context.Context, key string) (string, error) {
	c.ops = append(c.ops, w11aCacheOp{"get", key})
	return c.values[key], nil
}

func (c *w11aCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.ops = append(c.ops, w11aCacheOp{"set", key})
	c.values[key] = value
	return nil
}

func (c *w11aCache) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		c.ops = append(c.ops, w11aCacheOp{"del", k})
		delete(c.values, k)
	}
	return nil
}

func (c *w11aCache) SetNX(_ context.Context, key, _ string, _ time.Duration) (bool, error) {
	c.ops = append(c.ops, w11aCacheOp{"setnx", key})
	if _, held := c.values[key]; held {
		return false, nil
	}
	c.values[key] = "reserved"
	return true, nil
}

func (c *w11aCache) Ping(context.Context) error { return nil }

func (c *w11aCache) deletes(key string) int {
	n := 0
	for _, o := range c.ops {
		if o.Op == "del" && o.Key == key {
			n++
		}
	}
	return n
}

type w11aPrincipals struct {
	err     error
	tenants []uuid.UUID
}

func (p *w11aPrincipals) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	p.tenants = append(p.tenants, tenantID)
	return port.AutomationPrincipal{ID: w11aActor, Subject: "sub"}, p.err
}

type w11aWarn struct {
	Msg    string
	Fields map[string]any
}

type w11aLogger struct{ warns []w11aWarn }

func (l *w11aLogger) Debug(string, map[string]any) {}
func (l *w11aLogger) Info(string, map[string]any)  {}
func (l *w11aLogger) Warn(msg string, f map[string]any) {
	l.warns = append(l.warns, w11aWarn{msg, f})
}
func (l *w11aLogger) Error(string, map[string]any) {}

// w11aCallbacks serves the two connector callback routes from the real service
// over recording fakes.
type w11aCallbacks struct {
	router      http.Handler
	tenant      uuid.UUID
	task        *domain.Task
	tasks       *w11aTasks
	assignments *w11aAssignments
	temporal    *w11aTemporal
	cache       *w11aCache
	principals  *w11aPrincipals
	log         *w11aLogger
}

func w11aNewCallbacks(extras string) *w11aCallbacks {
	tenant := uuid.New()
	connectorType := "storage"
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenant, TemporalWorkflowID: "w11a:" + uuid.NewString(), Status: domain.InstanceStatusRunning}
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, NodeKey: "dept-9/node-A",
		Status: domain.TaskStatusReady, RecordVersion: 6, VisitCount: 4, ConnectorType: &connectorType,
		ExtrasJSON: json.RawMessage(extras),
	}
	h := &w11aCallbacks{
		tenant: tenant, task: task,
		tasks:       &w11aTasks{rows: []*domain.Task{task}},
		assignments: &w11aAssignments{},
		temporal:    &w11aTemporal{},
		cache:       &w11aCache{values: map[string]string{}},
		principals:  &w11aPrincipals{},
		log:         &w11aLogger{},
	}
	svc := &service.ConnectorTaskService{
		Instances: &w11aInstances{rows: []*domain.Instance{inst}}, Tasks: h.tasks, Assignments: h.assignments,
		Temporal: h.temporal, Cache: h.cache, Principals: h.principals, Log: h.log,
	}
	h.router = newInternalRouter(handler.New(handler.Services{ConnectorTasks: svc}))
	return h
}

func (h *w11aCallbacks) key(outcome string) string {
	return "connector-signal:" + outcome + ":" + h.task.ID.String()
}

func (h *w11aCallbacks) post(taskID uuid.UUID, action, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/internal/connector-tasks/"+taskID.String()+"/"+action, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, r)
	return w
}

func (h *w11aCallbacks) complete(extra string) *httptest.ResponseRecorder {
	return h.post(h.task.ID, "complete", fmt.Sprintf(`{"tenant_id":%q%s}`, h.tenant, extra))
}

func (h *w11aCallbacks) fail(extra string) *httptest.ResponseRecorder {
	return h.post(h.task.ID, "fail", fmt.Sprintf(`{"tenant_id":%q,"error_class":"upstream_error"%s}`, h.tenant, extra))
}

type w11aProblem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func w11aProblemOf(t *testing.T, w *httptest.ResponseRecorder, status int) w11aProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w11aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

func TestConnectorCallbacks_SignalFails_AnswersByCauseAndReleasesKey(t *testing.T) {
	signalCases := []struct {
		name       string
		cause      error
		wantStatus int
		wantCode   string
		wantDetail string
	}{
		{"workflow engine unavailable", fmt.Errorf("w11a: dial: %w", port.ErrTemporalUnavailable),
			http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "the workflow engine could not take the request; retry it"},
		{"signal timed out", fmt.Errorf("w11a: signal: %w", context.DeadlineExceeded),
			http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "request timed out"},
		{"any other failure", errW11aSignalRefused,
			http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred"},
	}
	for _, mapping := range []struct{ name, extras, outcome string }{
		{"complete", `{}`, "complete"},
		{"complete of a task with an unreadable stored mapping", `{not json`, "fail"},
	} {
		for _, tc := range signalCases {
			t.Run(mapping.name+"/"+tc.name, func(t *testing.T) {
				h := w11aNewCallbacks(mapping.extras)
				h.temporal.err = tc.cause

				w := h.complete(`,"output":{"k":"v"}`)

				p := w11aProblemOf(t, w, tc.wantStatus)
				assert.Equal(t, tc.wantCode, p.Code)
				assert.Equal(t, tc.wantDetail, p.Detail)
				require.Len(t, h.temporal.signals, 1)
				wantSignal := map[string]string{"complete": "stage-transition", "fail": "stage-fail"}[mapping.outcome]
				assert.Equal(t, wantSignal, h.temporal.signals[0].Name)
				_, held := h.cache.values[h.key(mapping.outcome)]
				assert.False(t, held, "the dedup key is released so the redelivery signals")
				assert.Equal(t, 1, h.cache.deletes(h.key(mapping.outcome)))
				require.Len(t, h.assignments.rows, 1, "the assignment row stays")
				assert.Equal(t, h.task.ID, h.assignments.rows[0].TaskID)
				assert.Equal(t, w11aActor, h.assignments.rows[0].UserID)
				assert.Equal(t, h.tenant, h.assignments.rows[0].TenantID)

				h.temporal.err = nil
				w = h.complete(`,"output":{"k":"v"}`)
				require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
				assert.Len(t, h.temporal.signals, 2, "the retry signals again")
			})
		}
	}
}

func TestConnectorCallbacks_UnknownOrForeignTask_Returns404TaskNotFound(t *testing.T) {
	for _, action := range []string{"complete", "fail"} {
		for _, scenario := range []string{"unknown task id", "task of another tenant"} {
			t.Run(action+"/"+scenario, func(t *testing.T) {
				h := w11aNewCallbacks(`{}`)
				callTenant, callTask := h.tenant, uuid.New()
				if scenario == "task of another tenant" {
					callTenant, callTask = uuid.New(), h.task.ID
				}
				body := fmt.Sprintf(`{"tenant_id":%q,"error_class":"upstream_error"}`, callTenant)

				w := h.post(callTask, action, body)

				p := w11aProblemOf(t, w, http.StatusNotFound)
				assert.Equal(t, "TASK_NOT_FOUND", p.Code)
				assert.Equal(t, []w11aRead{{callTenant, callTask}}, h.tasks.reads, "the read was scoped to the tenant in the body")
				assert.Empty(t, h.temporal.signals)
				assert.Empty(t, h.cache.ops)
				assert.Empty(t, h.assignments.attempts)
				assert.Empty(t, h.principals.tenants)
			})
		}
	}
}

func TestFailConnectorTask_PrincipalLookupFails_Returns409AndChangesNothing(t *testing.T) {
	h := w11aNewCallbacks(`{}`)
	h.principals.err = port.ErrNoAutomationPrincipal

	w := h.fail("")

	p := w11aProblemOf(t, w, http.StatusConflict)
	assert.Equal(t, "NO_AUTOMATION_PRINCIPAL", p.Code)
	assert.Equal(t, []uuid.UUID{h.tenant}, h.principals.tenants)
	assert.NotEmpty(t, h.cache.ops, "the earlier outcomes were read before the lookup")
	for _, op := range h.cache.ops {
		assert.Equal(t, "get", op.Op, "no dedup key is reserved or released: %v", op)
	}
	assert.Empty(t, h.cache.values, "no dedup key is left behind")
	assert.Empty(t, h.assignments.attempts, "no assignment is written")
	assert.Empty(t, h.temporal.signals, "no signal is sent")
}

func TestFailConnectorTask_OutputOmittedNullOrEmpty_Returns204WithNoKeysLogged(t *testing.T) {
	cases := []struct{ name, extra string }{
		{"omitted", ``},
		{"null", `,"output":null`},
		{"empty object", `,"output":{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w11aNewCallbacks(`{}`)

			w := h.fail(tc.extra)

			require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
			assert.Empty(t, w.Body.String())
			require.Len(t, h.temporal.signals, 1, "the failure is sent")
			assert.Equal(t, "stage-fail", h.temporal.signals[0].Name)
			var logged []w11aWarn
			for _, e := range h.log.warns {
				if _, ok := e.Fields["output_keys"]; ok {
					logged = append(logged, e)
				}
			}
			require.Len(t, logged, 1)
			keys, ok := logged[0].Fields["output_keys"].([]string)
			require.True(t, ok)
			assert.Empty(t, keys, "no output keys are logged")
		})
	}
}
