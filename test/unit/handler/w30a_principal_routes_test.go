package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type w30aTasks struct {
	port.TaskRepository
	row *domain.Task
}

func (r *w30aTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	if r.row.TenantID == tenantID && r.row.ID == id {
		return r.row, nil
	}
	return nil, domain.ErrNotFound
}

type w30aInstances struct {
	port.InstanceRepository
	row *domain.Instance
}

func (r *w30aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	if r.row.TenantID == tenantID && r.row.ID == id {
		return r.row, nil
	}
	return nil, domain.ErrNotFound
}

type w30aAssignments struct {
	port.TaskAssignmentRepository
	attempts []domain.TaskAssignment
}

func (r *w30aAssignments) Create(_ context.Context, a *domain.TaskAssignment) error {
	r.attempts = append(r.attempts, *a)
	return nil
}

type w30aTemporal struct {
	port.TemporalClient
	signals []string
}

func (c *w30aTemporal) SignalWorkflow(_ context.Context, _ string, _ uuid.UUID, name string, _ any) error {
	c.signals = append(c.signals, name)
	return nil
}

type w30aCache struct {
	ops []string
}

func (c *w30aCache) Get(_ context.Context, key string) (string, error) {
	c.ops = append(c.ops, "get "+key)
	return "", nil
}

func (c *w30aCache) Set(_ context.Context, key, _ string, _ time.Duration) error {
	c.ops = append(c.ops, "set "+key)
	return nil
}

func (c *w30aCache) Del(_ context.Context, keys ...string) error {
	c.ops = append(c.ops, "del "+strings.Join(keys, ","))
	return nil
}

func (c *w30aCache) SetNX(_ context.Context, key, _ string, _ time.Duration) (bool, error) {
	c.ops = append(c.ops, "setnx "+key)
	return true, nil
}

func (c *w30aCache) Ping(context.Context) error { return nil }

// w30aTokenService answers every read of the automation principal with one scripted answer and records the
// tenant each read named.
type w30aTokenService struct {
	srv     *httptest.Server
	mu      sync.Mutex
	tenants []string
}

func w30aNewTokenService(t *testing.T, status int, body string) *w30aTokenService {
	t.Helper()
	ts := &w30aTokenService{}
	ts.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts.mu.Lock()
		ts.tenants = append(ts.tenants, r.Header.Get("x-tenant-id"))
		ts.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.srv.Close)
	return ts
}

func (ts *w30aTokenService) reads() []string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]string(nil), ts.tenants...)
}

type w30aRoute struct {
	router      http.Handler
	tenant      uuid.UUID
	taskID      uuid.UUID
	assignments *w30aAssignments
	temporal    *w30aTemporal
	cache       *w30aCache
}

// w30aNewRoute serves both connector callback routes from the real service wired to the real principal client
// that reads tokenURL.
func w30aNewRoute(tokenURL string) *w30aRoute {
	tenant := uuid.New()
	connectorType := "storage"
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenant, TemporalWorkflowID: "w30a:" + uuid.NewString(), Status: domain.InstanceStatusRunning}
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, NodeKey: "dept-9/node-A",
		Status: domain.TaskStatusReady, RecordVersion: 3, ConnectorType: &connectorType,
		ExtrasJSON: json.RawMessage(`{}`),
	}
	r := &w30aRoute{
		tenant: tenant, taskID: task.ID,
		assignments: &w30aAssignments{}, temporal: &w30aTemporal{}, cache: &w30aCache{},
	}
	svc := &service.ConnectorTaskService{
		Instances: &w30aInstances{row: inst}, Tasks: &w30aTasks{row: task}, Assignments: r.assignments,
		Temporal: r.temporal, Cache: r.cache,
		Principals: adapterhttp.NewAutomationPrincipalClient(tokenURL, time.Second, time.Minute),
	}
	r.router = newInternalRouter(handler.New(handler.Services{ConnectorTasks: svc}))
	return r
}

func (r *w30aRoute) post(action string) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`{"tenant_id":%q,"error_class":"upstream_error"}`, r.tenant)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/connector-tasks/"+r.taskID.String()+"/"+action, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.router.ServeHTTP(w, req)
	return w
}

type w30aProblem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func w30aProblemOf(t *testing.T, w *httptest.ResponseRecorder, status int) w30aProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w30aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

const w30aUpstreamMetric = "workflow_execution_service_upstream_dependency_errors_total"

// A token service that cannot give a usable answer makes the callback a 503 the worker retries: nothing is
// reserved, assigned or signalled, the iam error counter moves once, and the cause text stays out of the body.
func TestConnectorCallbacks_TokenServiceGivesNoUsableAnswer_Returns503AndChangesNothing(t *testing.T) {
	obstest.Init(t)
	const principalID = "2b1f0000-0000-4000-8000-00000000000a"
	cases := []struct {
		name   string
		status int
		body   string
		// refused sends the call to a port nothing listens on instead of the scripted server.
		refused bool
	}{
		{name: "unreachable", refused: true},
		{name: "status 500", status: http.StatusInternalServerError},
		{name: "status 403", status: http.StatusForbidden, body: `{"principal_id":"` + principalID + `","principal_sub":"s","status":"active"}`},
		{name: "status 503", status: http.StatusServiceUnavailable},
		{name: "status 202", status: http.StatusAccepted, body: `{"principal_id":"` + principalID + `","principal_sub":"s","status":"active"}`},
		{name: "200 with a body that is not json", status: http.StatusOK, body: `<html>gateway</html>`},
		{name: "200 with an empty body", status: http.StatusOK, body: ``},
		{name: "200 without principal_id", status: http.StatusOK, body: `{"principal_sub":"s","status":"active"}`},
		{name: "200 with a null principal_id", status: http.StatusOK, body: `{"principal_id":null,"principal_sub":"s","status":"active"}`},
		{name: "200 with an empty principal_sub", status: http.StatusOK, body: `{"principal_id":"` + principalID + `","principal_sub":"","status":"active"}`},
		{name: "200 with principal_sub absent", status: http.StatusOK, body: `{"principal_id":"` + principalID + `","status":"active"}`},
	}
	for _, action := range []string{"complete", "fail"} {
		for _, tc := range cases {
			t.Run(action+"/"+tc.name, func(t *testing.T) {
				url := "http://127.0.0.1:1"
				var token *w30aTokenService
				if !tc.refused {
					token = w30aNewTokenService(t, tc.status, tc.body)
					url = token.srv.URL
				}
				route := w30aNewRoute(url)
				iamLabels := map[string]string{"dependency": "iam"}
				definitionLabels := map[string]string{"dependency": "definition_service"}
				iamBefore := obstest.Sum(t, w30aUpstreamMetric, iamLabels)
				definitionBefore := obstest.Sum(t, w30aUpstreamMetric, definitionLabels)

				w := route.post(action)

				p := w30aProblemOf(t, w, http.StatusServiceUnavailable)
				assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
				assert.Equal(t, "an upstream service could not serve the request", p.Detail)
				for _, leaked := range []string{"token service", "127.0.0.1", "principal_id", "principal_sub", "gateway"} {
					assert.NotContains(t, w.Body.String(), leaked, "the cause stays out of the body")
				}
				if token != nil {
					assert.Equal(t, []string{route.tenant.String()}, token.reads(), "one read of the calling tenant's principal")
				}
				assert.Equal(t, iamBefore+1, obstest.Sum(t, w30aUpstreamMetric, iamLabels), "the failure counts against iam")
				assert.Equal(t, definitionBefore, obstest.Sum(t, w30aUpstreamMetric, definitionLabels))
				assert.Empty(t, route.assignments.attempts, "no assignment is written")
				assert.Empty(t, route.temporal.signals, "no signal is sent")
				for _, op := range route.cache.ops {
					assert.True(t, strings.HasPrefix(op, "get "), "no dedup key is reserved or released: %s", op)
				}
			})
		}
	}
}
