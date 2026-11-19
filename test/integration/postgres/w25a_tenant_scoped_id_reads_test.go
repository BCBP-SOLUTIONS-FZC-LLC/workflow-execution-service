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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w25aReadProblem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func w25aAdminGet(router *gin.Engine, tenantID uuid.UUID, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("x-tenant-id", tenantID.String())
	r.Header.Set("x-user-id", uuid.NewString())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

// Against real row-level security an id route scopes its read by the caller's tenant: an admin of another tenant
// asking for this tenant's task or instance gets 404 (never 403 and never the row), while the owning tenant's
// admin reads it; the other tenant's task list holds nothing of it.
func TestIDRoutes_IDOfAnotherTenant_IsNotFoundAndNothingOfTheRowIsEchoed(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	outbox := postgres.NewOutboxRepo(appPool)
	h := handler.New(handler.Services{
		Tasks:     &service.TaskService{Tasks: tasks, Assignments: assignments},
		Instances: &service.InstanceService{Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outbox},
	})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.UseRawPath = true
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test"}) {
		router.Use(mw)
	}
	router.GET("/api/v1/tasks", h.ListTasks)
	router.GET("/api/v1/tasks/:id", h.GetTask)
	router.GET("/api/v1/instances/:id", h.GetInstance)
	ctx := context.Background()

	ownerTenant, otherTenant := uuid.New(), uuid.New()
	inst := newInstance(ownerTenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	task := newTask(ownerTenant, inst.ID)
	require.NoError(t, tasks.Create(ctx, task))

	cases := []struct {
		name     string
		path     string
		wantCode string
	}{
		{"get task", "/api/v1/tasks/" + task.ID.String(), "TASK_NOT_FOUND"},
		{"get instance", "/api/v1/instances/" + inst.ID.String(), "INSTANCE_NOT_FOUND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			own := w25aAdminGet(router, ownerTenant, tc.path)
			require.Equal(t, http.StatusOK, own.Code, "control: the owning tenant reads it: "+own.Body.String())

			other := w25aAdminGet(router, otherTenant, tc.path)

			require.Equal(t, http.StatusNotFound, other.Code, other.Body.String())
			var p w25aReadProblem
			require.NoError(t, json.Unmarshal(other.Body.Bytes(), &p))
			assert.Equal(t, http.StatusNotFound, p.Status)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.NotContains(t, other.Body.String(), ownerTenant.String(), "nothing of the other tenant's row is echoed")
			assert.NotContains(t, other.Body.String(), inst.SubjectID)
			assert.NotContains(t, other.Body.String(), task.NodeKey)
		})
	}

	t.Run("list tasks", func(t *testing.T) {
		own := w25aAdminGet(router, ownerTenant, "/api/v1/tasks")
		require.Equal(t, http.StatusOK, own.Code, own.Body.String())
		var ownBody struct {
			Items []struct {
				ID uuid.UUID `json:"id"`
			} `json:"items"`
		}
		require.NoError(t, json.Unmarshal(own.Body.Bytes(), &ownBody))
		require.Len(t, ownBody.Items, 1, "control: the owning tenant sees its task")
		assert.Equal(t, task.ID, ownBody.Items[0].ID)

		other := w25aAdminGet(router, otherTenant, "/api/v1/tasks")

		require.Equal(t, http.StatusOK, other.Code, other.Body.String())
		var otherBody struct {
			Items []json.RawMessage `json:"items"`
		}
		require.NoError(t, json.Unmarshal(other.Body.Bytes(), &otherBody))
		assert.Empty(t, otherBody.Items)
		assert.NotContains(t, other.Body.String(), task.ID.String())
	})
}
