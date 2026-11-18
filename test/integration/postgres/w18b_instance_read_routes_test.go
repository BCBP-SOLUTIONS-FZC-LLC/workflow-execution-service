//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w18bCaller struct {
	tenantID    uuid.UUID
	userID      uuid.UUID
	admin       bool
	departments string
}

func w18bGet(router *gin.Engine, caller w18bCaller, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("x-tenant-id", caller.tenantID.String())
	r.Header.Set("x-user-id", caller.userID.String())
	if caller.admin {
		r.Header.Set("x-tenant-roles", "tenant_admin")
	}
	if caller.departments != "" {
		r.Header.Set("x-departments", caller.departments)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

type w18bReadProblem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func w18bRouter(svc *service.InstanceService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.UseRawPath = true
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test"}) {
		r.Use(mw)
	}
	h := handler.New(handler.Services{Instances: svc})
	r.GET("/api/v1/instances", h.ListInstances)
	r.GET("/api/v1/instances/:id", h.GetInstance)
	r.GET("/api/v1/instances/:id/events", h.ListInstanceEvents)
	return r
}

type w18bReadWorld struct {
	superPool   *pgcommon.Pool
	router      *gin.Engine
	instances   *postgres.InstanceRepo
	tasks       *postgres.TaskRepo
	assignments *postgres.TaskAssignmentRepo
	transactor  *postgres.Transactor
	outbox      *postgres.OutboxRepo
}

func w18bNewReadWorld(t *testing.T) *w18bReadWorld {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	w := &w18bReadWorld{
		superPool:   superPool,
		instances:   postgres.NewInstanceRepo(appPool),
		tasks:       postgres.NewTaskRepo(appPool),
		assignments: postgres.NewTaskAssignmentRepo(appPool),
		transactor:  postgres.NewTransactor(appPool),
		outbox:      postgres.NewOutboxRepo(appPool),
	}
	w.router = w18bRouter(&service.InstanceService{
		Instances: w.instances, Tasks: w.tasks, Assignments: w.assignments, Outbox: w.outbox,
	})
	return w
}

func (w *w18bReadWorld) seedEvent(t *testing.T, tenantID, instanceID, taskID uuid.UUID, nodeKey string) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{"workflow_instance_id": instanceID.String(), "task_id": taskID.String(), "node_key": nodeKey})
	require.NoError(t, err)
	env := events.NewEnvelope(domain.EventWorkflowTaskClaimed, "execution-service", json.RawMessage(data), events.WithTenantID(tenantID.String()))
	require.NoError(t, w.transactor.RunInTx(withGUC(context.Background(), tenantID), func(ctx context.Context) error {
		return w.outbox.Enqueue(ctx, env)
	}))
	return env.ID
}

func w18bEventIDs(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Items []struct {
			ID     string     `json:"id"`
			TaskID *uuid.UUID `json:"task_id"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ids := make([]string, len(body.Items))
	for i, it := range body.Items {
		ids[i] = it.ID
		assert.NotNil(t, it.TaskID, "the task id of a task event is taken from its data")
	}
	return ids
}

func TestInstanceReadRoutes_ScopeAndTenantRefusalsAgainstPostgres(t *testing.T) {
	world := w18bNewReadWorld(t)
	ctx := context.Background()

	t.Run("a non-admin on an instance with no tasks is refused with 403, an admin reads it", func(t *testing.T) {
		tenantID := uuid.New()
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, world.instances.Create(ctx, inst))
		member := w18bCaller{tenantID: tenantID, userID: uuid.New(), departments: uuid.NewString() + ":member"}
		admin := w18bCaller{tenantID: tenantID, userID: uuid.New(), admin: true}

		for _, path := range []string{"/api/v1/instances/" + inst.ID.String(), "/api/v1/instances/" + inst.ID.String() + "/events"} {
			rec := w18bGet(world.router, member, path)

			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			var p w18bReadProblem
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
			assert.Equal(t, "NOT_AUTHORIZED_FOR_RESOURCE", p.Code, path)
		}

		rec := w18bGet(world.router, admin, "/api/v1/instances/"+inst.ID.String())
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var detail struct {
			ID    uuid.UUID         `json:"id"`
			Tasks []json.RawMessage `json:"tasks"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detail))
		assert.Equal(t, inst.ID, detail.ID)
		assert.Empty(t, detail.Tasks)
	})

	t.Run("an admin of another tenant asking for this tenant's instance gets 404 and nothing of it", func(t *testing.T) {
		ownerTenant, otherTenant := uuid.New(), uuid.New()
		inst := newInstance(ownerTenant, time.Now().UTC())
		require.NoError(t, world.instances.Create(ctx, inst))
		task := newTask(ownerTenant, inst.ID)
		require.NoError(t, world.tasks.Create(ctx, task))
		world.seedEvent(t, ownerTenant, inst.ID, task.ID, task.NodeKey)
		outsider := w18bCaller{tenantID: otherTenant, userID: uuid.New(), admin: true}
		owner := w18bCaller{tenantID: ownerTenant, userID: uuid.New(), admin: true}

		for _, path := range []string{"/api/v1/instances/" + inst.ID.String(), "/api/v1/instances/" + inst.ID.String() + "/events"} {
			rec := w18bGet(world.router, outsider, path)

			require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
			var p w18bReadProblem
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
			assert.Equal(t, "INSTANCE_NOT_FOUND", p.Code, path)
			assert.NotContains(t, rec.Body.String(), inst.SubjectID, "nothing of the other tenant's instance is echoed")
			assert.NotContains(t, rec.Body.String(), task.ID.String())

			assert.Equal(t, http.StatusOK, w18bGet(world.router, owner, path).Code, "control: the owning tenant's admin reads it at "+path)
		}
	})

	t.Run("an active assignee reads the whole instance and its events, whatever the departments of the other tasks", func(t *testing.T) {
		tenantID := uuid.New()
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, world.instances.Create(ctx, inst))
		assignee := uuid.New()
		var taskIDs []string
		var eventIDs []string
		for i := 0; i < 3; i++ {
			task := newTask(tenantID, inst.ID)
			task.NodeKey = "dept" + string(rune('a'+i)) + "/review"
			require.NoError(t, world.tasks.Create(ctx, task))
			if i == 1 {
				require.NoError(t, world.assignments.Create(ctx, newTaskAssignment(tenantID, task.ID, assignee)))
			}
			taskIDs = append(taskIDs, task.ID.String())
			eventIDs = append(eventIDs, world.seedEvent(t, tenantID, inst.ID, task.ID, task.NodeKey))
		}
		byAssignment := w18bCaller{tenantID: tenantID, userID: assignee}
		admin := w18bCaller{tenantID: tenantID, userID: uuid.New(), admin: true}
		stranger := w18bCaller{tenantID: tenantID, userID: uuid.New(), departments: uuid.NewString() + ":member"}

		rec := w18bGet(world.router, byAssignment, "/api/v1/instances/"+inst.ID.String())
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var detail struct {
			Tasks []struct {
				ID           string `json:"id"`
				DepartmentID string `json:"department_id"`
			} `json:"tasks"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detail))
		var gotTaskIDs, gotDepartments []string
		for _, tk := range detail.Tasks {
			gotTaskIDs = append(gotTaskIDs, tk.ID)
			gotDepartments = append(gotDepartments, tk.DepartmentID)
		}
		require.Len(t, detail.Tasks, 3, "every task of the instance is returned, not only the assignee's")
		require.ElementsMatch(t, taskIDs, gotTaskIDs)
		distinct := map[string]bool{}
		for _, d := range gotDepartments {
			distinct[d] = true
		}
		assert.Len(t, distinct, 3, "the tasks belong to three departments")

		eventsPath := "/api/v1/instances/" + inst.ID.String() + "/events"
		assigneeEvents := w18bEventIDs(t, w18bGet(world.router, byAssignment, eventsPath))
		adminEvents := w18bEventIDs(t, w18bGet(world.router, admin, eventsPath))
		assert.ElementsMatch(t, eventIDs, adminEvents)
		assert.Equal(t, adminEvents, assigneeEvents, "the assignee gets the same event page as the admin")

		refused := w18bGet(world.router, stranger, eventsPath)
		assert.Equal(t, http.StatusForbidden, refused.Code, "control: a caller with neither an assignment nor the department is refused")
	})

	t.Run("a department member reads the same event page as the admin", func(t *testing.T) {
		tenantID := uuid.New()
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, world.instances.Create(ctx, inst))
		task := newTask(tenantID, inst.ID)
		require.NoError(t, world.tasks.Create(ctx, task))
		eventID := world.seedEvent(t, tenantID, inst.ID, task.ID, task.NodeKey)
		member := w18bCaller{tenantID: tenantID, userID: uuid.New(), departments: task.DepartmentID.String() + ":member"}

		got := w18bEventIDs(t, w18bGet(world.router, member, "/api/v1/instances/"+inst.ID.String()+"/events"))

		assert.Equal(t, []string{eventID}, got)
	})

	t.Run("the instance list and the event list carry no next_cursor key on a last page and one when more remain", func(t *testing.T) {
		tenantID := uuid.New()
		admin := w18bCaller{tenantID: tenantID, userID: uuid.New(), admin: true}
		base := time.Now().UTC().Add(-time.Hour)
		var instanceIDs []string
		var inst *domain.Instance
		for i := 0; i < 3; i++ {
			inst = newInstance(tenantID, base)
			require.NoError(t, world.instances.Create(ctx, inst))
			seedInstanceAt(t, world.superPool, ctx, inst.ID, base.Add(time.Duration(i)*time.Minute))
			instanceIDs = append(instanceIDs, inst.ID.String())
		}
		task := newTask(tenantID, inst.ID)
		require.NoError(t, world.tasks.Create(ctx, task))
		var eventIDs []string
		for i := 0; i < 3; i++ {
			eventIDs = append(eventIDs, world.seedEvent(t, tenantID, inst.ID, task.ID, task.NodeKey))
		}

		decodePage := func(rec *httptest.ResponseRecorder) (ids []string, cursor string, hasCursor bool) {
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var keys map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &keys))
			var items []struct {
				ID string `json:"id"`
			}
			require.NoError(t, json.Unmarshal(keys["items"], &items))
			for _, it := range items {
				ids = append(ids, it.ID)
			}
			raw, ok := keys["next_cursor"]
			if ok {
				require.NoError(t, json.Unmarshal(raw, &cursor))
			}
			return ids, cursor, ok
		}

		t.Run("instances: three rows with limit 3 end the list", func(t *testing.T) {
			ids, _, hasCursor := decodePage(w18bGet(world.router, admin, "/api/v1/instances?limit=3"))
			require.ElementsMatch(t, instanceIDs, ids)
			assert.False(t, hasCursor, "a page that holds every remaining row has no next_cursor key")
		})

		t.Run("instances: limit 2 of 3 rows has a cursor and the next page ends the list", func(t *testing.T) {
			first, cursor, hasCursor := decodePage(w18bGet(world.router, admin, "/api/v1/instances?limit=2"))
			require.Len(t, first, 2)
			require.True(t, hasCursor, "more rows remain")
			require.NotEmpty(t, cursor)
			second, _, hasMore := decodePage(w18bGet(world.router, admin, "/api/v1/instances?limit=2&cursor="+cursor))
			require.Len(t, second, 1)
			assert.False(t, hasMore, "the last page has no next_cursor key")
			assert.ElementsMatch(t, instanceIDs, append(append([]string{}, first...), second...))
		})

		eventsPath := "/api/v1/instances/" + inst.ID.String() + "/events"
		t.Run("events: limit 3 of 3 events ends the list", func(t *testing.T) {
			ids, _, hasCursor := decodePage(w18bGet(world.router, admin, eventsPath+"?limit=3"))
			require.Len(t, ids, 3)
			assert.False(t, hasCursor)
		})

		t.Run("events: limit 2 of 3 has a cursor and the next page ends the list", func(t *testing.T) {
			first, cursor, hasCursor := decodePage(w18bGet(world.router, admin, eventsPath+"?limit=2"))
			require.Len(t, first, 2)
			require.True(t, hasCursor)
			second, _, hasMore := decodePage(w18bGet(world.router, admin, eventsPath+"?limit=2&cursor="+cursor))
			require.Len(t, second, 1)
			assert.False(t, hasMore)
			assert.ElementsMatch(t, eventIDs, append(append([]string{}, first...), second...))
		})
	})
}
