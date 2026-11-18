//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskRepo_ListByTenant_ScopeIgnoresEndedAssignments(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	type world struct {
		tenantID                                    uuid.UUID
		deptTask, activeTask, vacatedTask, doneTask *domain.Task
		active, vacated, done                       uuid.UUID
	}
	seed := func(t *testing.T) world {
		t.Helper()
		w := world{tenantID: uuid.New(), active: uuid.New(), vacated: uuid.New(), done: uuid.New()}
		inst := newInstance(w.tenantID, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		mk := func(node string) *domain.Task {
			task := newTask(w.tenantID, inst.ID)
			task.NodeKey = node
			require.NoError(t, taskRepo.Create(ctx, task))
			return task
		}
		w.deptTask, w.activeTask, w.vacatedTask, w.doneTask = mk("dept/only"), mk("active/held"), mk("vacated/held"), mk("done/held")

		require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(w.tenantID, w.activeTask.ID, w.active)))
		vacated := newTaskAssignment(w.tenantID, w.vacatedTask.ID, w.vacated)
		require.NoError(t, assignmentRepo.Create(ctx, vacated))
		_, err := assignmentRepo.Vacate(ctx, w.tenantID, vacated.ID)
		require.NoError(t, err)
		done := newTaskAssignment(w.tenantID, w.doneTask.ID, w.done)
		require.NoError(t, assignmentRepo.Create(ctx, done))
		got, err := taskRepo.GetByID(ctx, w.tenantID, w.doneTask.ID)
		require.NoError(t, err)
		_, err = assignmentRepo.Complete(ctx, w.tenantID, done.ID, json.RawMessage(`{}`), got.RecordVersion)
		require.NoError(t, err)
		return w
	}
	listIDs := func(t *testing.T, tenantID uuid.UUID, scope *port.ScopeFilter) []uuid.UUID {
		t.Helper()
		tasks, _, err := taskRepo.ListByTenant(ctx, tenantID, port.TaskListFilter{Scope: scope}, port.PageRequest{Limit: 50})
		require.NoError(t, err)
		ids := make([]uuid.UUID, len(tasks))
		for i, task := range tasks {
			ids[i] = task.ID
		}
		return ids
	}

	t.Run("a caller with an active assignment sees that task only", func(t *testing.T) {
		w := seed(t)
		got := listIDs(t, w.tenantID, &port.ScopeFilter{DepartmentIDs: []uuid.UUID{uuid.New()}, CallerUserID: w.active})
		assert.Equal(t, []uuid.UUID{w.activeTask.ID}, got)
	})

	t.Run("a caller whose assignment was vacated sees nothing", func(t *testing.T) {
		w := seed(t)
		got := listIDs(t, w.tenantID, &port.ScopeFilter{DepartmentIDs: []uuid.UUID{uuid.New()}, CallerUserID: w.vacated})
		assert.Empty(t, got)
	})

	t.Run("a caller whose assignment is completed sees nothing", func(t *testing.T) {
		w := seed(t)
		got := listIDs(t, w.tenantID, &port.ScopeFilter{DepartmentIDs: []uuid.UUID{uuid.New()}, CallerUserID: w.done})
		assert.Empty(t, got)
	})

	t.Run("a department listed in the scope shows its tasks whatever the assignments", func(t *testing.T) {
		w := seed(t)
		got := listIDs(t, w.tenantID, &port.ScopeFilter{DepartmentIDs: []uuid.UUID{w.deptTask.DepartmentID}, CallerUserID: w.vacated})
		assert.Equal(t, []uuid.UUID{w.deptTask.ID}, got, "the vacated assignment adds nothing to the department's own task")
	})

	t.Run("another tenant's caller sees none of this tenant's tasks, only their own", func(t *testing.T) {
		a, b := seed(t), seed(t)
		scope := &port.ScopeFilter{
			DepartmentIDs: []uuid.UUID{a.deptTask.DepartmentID, b.deptTask.DepartmentID},
			CallerUserID:  a.active,
		}

		got := listIDs(t, b.tenantID, scope)

		assert.ElementsMatch(t, []uuid.UUID{b.deptTask.ID}, got, "tenant B's own department task is listed")
		for _, id := range []uuid.UUID{a.deptTask.ID, a.activeTask.ID, a.vacatedTask.ID, a.doneTask.ID} {
			assert.NotContains(t, got, id, "a task of tenant A must not be listed to tenant B")
		}
	})
}
