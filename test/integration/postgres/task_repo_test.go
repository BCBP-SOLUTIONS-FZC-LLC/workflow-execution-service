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
	})

	t.Run("stale record_version returns a conflict, not not-found", func(t *testing.T) {
		_, err := taskRepo.UpdateStatus(ctx, tenantA, task.ID, domain.TaskStatusCompleted, task.RecordVersion)
		assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	})
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
