//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w25aSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
}

type w25aSignals struct {
	port.TemporalClient
	mu   sync.Mutex
	sent []w25aSignal
}

func (c *w25aSignals) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, _ any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, w25aSignal{workflowID, instanceID, name})
	return nil
}

func (c *w25aSignals) signals() []w25aSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]w25aSignal(nil), c.sent...)
}

// w25aPrincipal returns a fixed principal and records the tenant it was asked for.
type w25aPrincipal struct {
	id      uuid.UUID
	mu      sync.Mutex
	tenants []uuid.UUID
}

func (p *w25aPrincipal) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenants = append(p.tenants, tenantID)
	return port.AutomationPrincipal{ID: p.id, Subject: "automation"}, nil
}

func (p *w25aPrincipal) askedTenants() []uuid.UUID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uuid.UUID(nil), p.tenants...)
}

type w25aHealthy struct{}

func (w25aHealthy) Ping(context.Context) error { return nil }
func (w25aHealthy) Health(context.Context) inboundhttp.DBHealth {
	return inboundhttp.DBHealth{Healthy: true}
}

// w25aSystemCaller sets the headers the internal group admits: the system role and a user id. The tenant header,
// when given, names a tenant that the routes must ignore.
func w25aSystemCaller(r *http.Request, headerTenant string) {
	r.Header.Set("x-tenant-roles", "iam-system")
	r.Header.Set("x-user-id", uuid.NewString())
	if headerTenant != "" {
		r.Header.Set("x-tenant-id", headerTenant)
	}
}

func w25aInternalPost(router http.Handler, path, headerTenant string, body any) *httptest.ResponseRecorder {
	raw, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
	r.Header.Set("Content-Type", "application/json")
	w25aSystemCaller(r, headerTenant)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func w25aInternalGet(router http.Handler, path, headerTenant string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	w25aSystemCaller(r, headerTenant)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

// The internal routes take the tenant from the body or the query and never from the x-tenant-id header, against
// real row-level security: a callback for another tenant's connector task is 404 with nothing signalled or
// assigned, whatever the header says; the active-instance count and the pause of a tenant that holds nothing are
// 0 and 204 with no signal; the owning tenant's body gets the work done even under another tenant's header.
func TestInternalRoutes_TenantComesFromTheBodyOrQuery_NeverFromTheHeader(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	temporal := &w25aSignals{}
	principals := &w25aPrincipal{id: uuid.New()}
	principal := principals.id
	h := handler.New(handler.Services{
		ConnectorTasks: &service.ConnectorTaskService{
			Instances: instances, Tasks: tasks, Assignments: assignments, Temporal: temporal, Principals: principals,
		},
		ArchiveGuard:   &service.ArchiveGuard{Instances: instances},
		UserTaskPauser: &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal},
	})
	gin.SetMode(gin.TestMode)
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test",
		},
		AppEnv:   "test",
		Handler:  h,
		DB:       w25aHealthy{},
		Cache:    w25aHealthy{},
		Temporal: w25aHealthy{},
	}).Handler()
	ctx := context.Background()

	owner, other := uuid.New(), uuid.New()
	workflowID := uuid.New()
	inst := newInstance(owner, time.Now().UTC())
	inst.WorkflowID = workflowID
	require.NoError(t, instances.Create(ctx, inst))
	connector := "storage"
	task := newTask(owner, inst.ID)
	task.ConnectorType = &connector
	require.NoError(t, tasks.Create(ctx, task))
	holder := uuid.New()
	require.NoError(t, assignments.Create(ctx, newTaskAssignment(owner, task.ID, holder)))

	t.Run("complete and fail of another tenant's task are 404 and change nothing", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			path string
			body map[string]any
		}{
			{"complete", "/api/v1/internal/connector-tasks/" + task.ID.String() + "/complete", map[string]any{"tenant_id": other, "output": map[string]any{}}},
			{"fail", "/api/v1/internal/connector-tasks/" + task.ID.String() + "/fail", map[string]any{"tenant_id": other, "error_class": "provider_down"}},
		} {
			// The header names the owning tenant; the route must ignore it and use the body's tenant.
			w := w25aInternalPost(router, tc.path, owner.String(), tc.body)

			require.Equal(t, http.StatusNotFound, w.Code, tc.name+": "+w.Body.String())
			var p w25aReadProblem
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p))
			assert.Equal(t, "TASK_NOT_FOUND", p.Code, tc.name)
			assert.NotContains(t, w.Body.String(), owner.String(), tc.name)
		}
		assert.Empty(t, temporal.signals(), "nothing was signalled for a foreign task")
		got, err := assignments.ListActiveByTask(ctx, owner, task.ID)
		require.NoError(t, err)
		require.Len(t, got, 1, "no automation assignment was written")
		assert.Equal(t, holder, got[0].UserID)
	})

	t.Run("active-instances counts the queried tenant only", func(t *testing.T) {
		path := func(tenant uuid.UUID) string {
			return "/api/v1/internal/workflows/" + workflowID.String() + "/active-instances?tenant_id=" + tenant.String()
		}
		for _, header := range []string{"", owner.String(), other.String()} {
			emptyTenant := w25aInternalGet(router, path(other), header)
			require.Equal(t, http.StatusOK, emptyTenant.Code, emptyTenant.Body.String())
			assert.JSONEq(t, `{"has_active":false,"count":0}`, emptyTenant.Body.String(), "header %q is ignored", header)

			ownTenant := w25aInternalGet(router, path(owner), header)
			require.Equal(t, http.StatusOK, ownTenant.Code, ownTenant.Body.String())
			assert.JSONEq(t, `{"has_active":true,"count":1}`, ownTenant.Body.String(), "header %q is ignored", header)
		}
	})

	t.Run("pause-tasks of a tenant that holds nothing is 204 and signals nothing", func(t *testing.T) {
		path := "/api/v1/internal/users/" + holder.String() + "/pause-tasks"

		w := w25aInternalPost(router, path, owner.String(), map[string]any{"tenant_id": other})

		require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
		assert.Empty(t, temporal.signals(), "the user holds nothing in the other tenant")
	})

	t.Run("the owning tenant's body does the work under another tenant's header", func(t *testing.T) {
		pause := w25aInternalPost(router, "/api/v1/internal/users/"+holder.String()+"/pause-tasks", other.String(), map[string]any{"tenant_id": owner})
		require.Equal(t, http.StatusNoContent, pause.Code, pause.Body.String())
		assert.Equal(t, []w25aSignal{{inst.TemporalWorkflowID, inst.ID, port.SignalInstancePause}}, temporal.signals())

		complete := w25aInternalPost(router, "/api/v1/internal/connector-tasks/"+task.ID.String()+"/complete", other.String(),
			map[string]any{"tenant_id": owner, "output": map[string]any{}})
		require.Equal(t, http.StatusNoContent, complete.Code, complete.Body.String())
		sent := temporal.signals()
		require.Len(t, sent, 2)
		assert.Equal(t, w25aSignal{inst.TemporalWorkflowID, inst.ID, "stage-transition"}, sent[1])
		got, err := assignments.ListActiveByTask(ctx, owner, task.ID)
		require.NoError(t, err)
		var users []uuid.UUID
		for _, a := range got {
			users = append(users, a.UserID)
		}
		assert.ElementsMatch(t, []uuid.UUID{holder, principal}, users, "the automation principal was assigned in the owning tenant")
		assert.Equal(t, []uuid.UUID{owner}, principals.askedTenants(), "the principal was asked for the body's tenant, not the header's")
	})
}
