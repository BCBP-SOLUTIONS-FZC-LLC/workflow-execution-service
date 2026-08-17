//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func newTask(tenantID, instanceID uuid.UUID) *domain.Task {
	return &domain.Task{
		ID:                 uuid.New(),
		TenantID:           tenantID,
		WorkflowInstanceID: instanceID,
		NodeKey:            "finance/review",
		DepartmentID:       uuid.New(),
		Status:             domain.TaskStatusReady,
		AssigneeMode:       "single",
	}
}

func seedTaskAt(t *testing.T, superPool *pgcommon.Pool, ctx context.Context, id uuid.UUID, createdAt time.Time) {
	t.Helper()
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE workflow_task SET created_at = $2 WHERE id = $1`, id, createdAt)
		return err
	}))
}

func TestTaskRepo_CreateAndGetByID(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	tenantB := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))

	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))
	assert.Equal(t, int64(1), task.RecordVersion)

	t.Run("same tenant can read it back", func(t *testing.T) {
		got, err := taskRepo.GetByID(ctx, tenantA, task.ID)
		require.NoError(t, err)
		assert.Equal(t, task.NodeKey, got.NodeKey)
	})

	t.Run("cross tenant sees not found", func(t *testing.T) {
		_, err := taskRepo.GetByID(ctx, tenantB, task.ID)
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("a task against a nonexistent instance is rejected", func(t *testing.T) {
		orphan := newTask(tenantA, uuid.New())
		assert.Error(t, taskRepo.Create(ctx, orphan))
	})
}

func TestTaskRepo_UpdateStatus(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))

	t.Run("correct record_version succeeds and bumps it", func(t *testing.T) {
		updated, err := taskRepo.UpdateStatus(ctx, tenantA, task.ID, domain.TaskStatusInProgress, task.RecordVersion)
		require.NoError(t, err)
		assert.Equal(t, domain.TaskStatusInProgress, updated.Status)
		assert.Equal(t, task.RecordVersion+1, updated.RecordVersion)
		assert.Nil(t, updated.CompletedAt, "a non-terminal transition must not set completed_at")
	})

	t.Run("stale record_version returns a conflict, not not-found", func(t *testing.T) {
		_, err := taskRepo.UpdateStatus(ctx, tenantA, task.ID, domain.TaskStatusCompleted, task.RecordVersion)
		assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	})
}

func TestTaskRepo_UpdateStatus_SetsCompletedAtOnlyForTerminalStatuses(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	for _, status := range []domain.TaskStatus{
		domain.TaskStatusCompleted, domain.TaskStatusDeferred, domain.TaskStatusFailed, domain.TaskStatusSuperseded,
	} {
		t.Run(string(status), func(t *testing.T) {
			tenantA := uuid.New()
			inst := newInstance(tenantA, time.Now().UTC())
			require.NoError(t, instanceRepo.Create(ctx, inst))
			task := newTask(tenantA, inst.ID)
			require.NoError(t, taskRepo.Create(ctx, task))

			updated, err := taskRepo.UpdateStatus(ctx, tenantA, task.ID, status, task.RecordVersion)
			require.NoError(t, err)
			require.NotNil(t, updated.CompletedAt)
			assert.WithinDuration(t, time.Now().UTC(), *updated.CompletedAt, 5*time.Second)
		})
	}
}

func TestTaskRepo_UpdateStatus_InvalidEnumSurfacesError(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))

	_, err := taskRepo.UpdateStatus(ctx, tenantA, task.ID, domain.TaskStatus("NOT_A_REAL_STATUS"), task.RecordVersion)
	assert.Error(t, err)
}

func TestTaskRepo_ListByInstance_KeysetPagination(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))

	base := time.Now().UTC().Add(-time.Hour)
	var seeded []uuid.UUID
	for i := 0; i < 4; i++ {
		task := newTask(tenantA, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		seedTaskAt(t, superPool, ctx, task.ID, base.Add(time.Duration(i)*time.Minute))
		seeded = append(seeded, task.ID)
	}

	var collected []uuid.UUID
	var cursor *port.Cursor
	for {
		page, next, err := taskRepo.ListByInstance(ctx, tenantA, inst.ID, port.PageRequest{After: cursor, Limit: 2})
		require.NoError(t, err)
		for _, task := range page {
			collected = append(collected, task.ID)
		}
		if next == nil {
			break
		}
		cursor = next
	}

	require.Len(t, collected, 4)
	for i, id := range collected {
		assert.Equal(t, seeded[3-i], id, "page order mismatch at position %d", i)
	}
}

func TestTaskRepo_ListByTenant_Filters(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	instA := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, instA))

	ready := newTask(tenantA, instA.ID)
	require.NoError(t, taskRepo.Create(ctx, ready))

	inProgress := newTask(tenantA, instA.ID)
	require.NoError(t, taskRepo.Create(ctx, inProgress))
	_, err := taskRepo.UpdateStatus(ctx, tenantA, inProgress.ID, domain.TaskStatusInProgress, inProgress.RecordVersion)
	require.NoError(t, err)

	assignee := uuid.New()
	require.NoError(t, assignmentRepo.Create(ctx, &domain.TaskAssignment{
		ID: uuid.New(), TenantID: tenantA, TaskID: ready.ID, UserID: assignee,
	}))

	otherTenantTask := newTask(tenantB, instA.ID)
	instB := newInstance(tenantB, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, instB))
	otherTenantTask.WorkflowInstanceID = instB.ID
	require.NoError(t, taskRepo.Create(ctx, otherTenantTask))

	t.Run("unfiltered is tenant-scoped only", func(t *testing.T) {
		tasks, _, err := taskRepo.ListByTenant(ctx, tenantA, port.TaskListFilter{}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		assert.Len(t, tasks, 2)
	})

	t.Run("status filter", func(t *testing.T) {
		status := domain.TaskStatusInProgress
		tasks, _, err := taskRepo.ListByTenant(ctx, tenantA, port.TaskListFilter{Status: &status}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		assert.Equal(t, inProgress.ID, tasks[0].ID)
	})

	t.Run("workflow_instance_id filter", func(t *testing.T) {
		instC := newInstance(tenantA, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, instC))
		otherInstanceTask := newTask(tenantA, instC.ID)
		require.NoError(t, taskRepo.Create(ctx, otherInstanceTask))

		tasks, _, err := taskRepo.ListByTenant(ctx, tenantA, port.TaskListFilter{WorkflowInstanceID: &instC.ID}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		assert.Equal(t, otherInstanceTask.ID, tasks[0].ID)
	})

	t.Run("due_before filter", func(t *testing.T) {
		soonDue := newTask(tenantA, instA.ID)
		soon := time.Now().UTC().Add(time.Hour)
		soonDue.DueAt = &soon
		require.NoError(t, taskRepo.Create(ctx, soonDue))

		laterDue := newTask(tenantA, instA.ID)
		later := time.Now().UTC().Add(48 * time.Hour)
		laterDue.DueAt = &later
		require.NoError(t, taskRepo.Create(ctx, laterDue))

		cutoff := time.Now().UTC().Add(24 * time.Hour)
		tasks, _, err := taskRepo.ListByTenant(ctx, tenantA, port.TaskListFilter{DueBefore: &cutoff}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		var ids []uuid.UUID
		for _, task := range tasks {
			ids = append(ids, task.ID)
		}
		assert.Contains(t, ids, soonDue.ID)
		assert.NotContains(t, ids, laterDue.ID)
	})

	t.Run("keyset pagination", func(t *testing.T) {
		tenantC := uuid.New()
		instD := newInstance(tenantC, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, instD))

		base := time.Now().UTC().Add(-time.Hour)
		var seeded []uuid.UUID
		for i := 0; i < 3; i++ {
			task := newTask(tenantC, instD.ID)
			require.NoError(t, taskRepo.Create(ctx, task))
			seedTaskAt(t, superPool, ctx, task.ID, base.Add(time.Duration(i)*time.Minute))
			seeded = append(seeded, task.ID)
		}

		firstPage, next, err := taskRepo.ListByTenant(ctx, tenantC, port.TaskListFilter{}, port.PageRequest{Limit: 2})
		require.NoError(t, err)
		require.Len(t, firstPage, 2)
		require.NotNil(t, next)

		secondPage, next2, err := taskRepo.ListByTenant(ctx, tenantC, port.TaskListFilter{}, port.PageRequest{After: next, Limit: 2})
		require.NoError(t, err)
		require.Len(t, secondPage, 1)
		assert.Nil(t, next2)
		assert.Equal(t, seeded[0], secondPage[0].ID)
	})

	t.Run("assignee filter", func(t *testing.T) {
		tasks, _, err := taskRepo.ListByTenant(ctx, tenantA, port.TaskListFilter{AssigneeUserID: &assignee}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		assert.Equal(t, ready.ID, tasks[0].ID)
	})

	t.Run("department filter matching nothing returns empty, not error", func(t *testing.T) {
		noMatch := uuid.New()
		tasks, _, err := taskRepo.ListByTenant(ctx, tenantA, port.TaskListFilter{DepartmentID: &noMatch}, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		assert.Empty(t, tasks)
	})
}

func TestTaskRepo_GetByInstanceAndNode(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))

	t.Run("no task at this node is not-found", func(t *testing.T) {
		_, err := taskRepo.GetByInstanceAndNode(ctx, tenantA, inst.ID, "finance/review")
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	first := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, first))
	seedTaskAt(t, superPool, ctx, first.ID, time.Now().UTC().Add(-time.Hour))

	second := newTask(tenantA, inst.ID) // same node_key ("finance/review") — a revisit
	require.NoError(t, taskRepo.Create(ctx, second))

	t.Run("returns the most recently created task at the node", func(t *testing.T) {
		got, err := taskRepo.GetByInstanceAndNode(ctx, tenantA, inst.ID, "finance/review")
		require.NoError(t, err)
		assert.Equal(t, second.ID, got.ID)
	})
}
