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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func newTaskAssignment(tenantID, taskID, userID uuid.UUID) *domain.TaskAssignment {
	return &domain.TaskAssignment{
		ID:       uuid.New(),
		TenantID: tenantID,
		TaskID:   taskID,
		UserID:   userID,
	}
}

func TestTaskAssignmentRepo_CreateListVacate(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	userID := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))

	assignment := newTaskAssignment(tenantA, task.ID, userID)
	require.NoError(t, assignmentRepo.Create(ctx, assignment))
	assert.True(t, assignment.IsActive)

	t.Run("shows up in both active-by-task and active-by-user", func(t *testing.T) {
		byTask, err := assignmentRepo.ListActiveByTask(ctx, tenantA, task.ID)
		require.NoError(t, err)
		require.Len(t, byTask, 1)
		assert.Equal(t, assignment.ID, byTask[0].ID)

		byUser, err := assignmentRepo.ListActiveByUser(ctx, tenantA, userID)
		require.NoError(t, err)
		require.Len(t, byUser, 1)
		assert.Equal(t, assignment.ID, byUser[0].ID)
	})

	t.Run("a second active assignment for the same (task,user) pair is rejected", func(t *testing.T) {
		dup := newTaskAssignment(tenantA, task.ID, userID)
		err := assignmentRepo.Create(ctx, dup)
		assert.ErrorIs(t, err, domain.ErrDuplicateActiveAssignment)
	})

	t.Run("GetByID reads it back, cross-tenant sees not-found", func(t *testing.T) {
		got, err := assignmentRepo.GetByID(ctx, tenantA, assignment.ID)
		require.NoError(t, err)
		assert.Equal(t, assignment.ID, got.ID)

		_, err = assignmentRepo.GetByID(ctx, uuid.New(), assignment.ID)
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("vacate deactivates and drops it from active lists", func(t *testing.T) {
		vacated, err := assignmentRepo.Vacate(ctx, tenantA, assignment.ID)
		require.NoError(t, err)
		assert.False(t, vacated.IsActive)
		assert.NotNil(t, vacated.VacatedAt)

		byTask, err := assignmentRepo.ListActiveByTask(ctx, tenantA, task.ID)
		require.NoError(t, err)
		assert.Empty(t, byTask)

		// vacating frees the (task_id, user_id) pair for a fresh assignment.
		fresh := newTaskAssignment(tenantA, task.ID, userID)
		require.NoError(t, assignmentRepo.Create(ctx, fresh))
	})
}

func TestTaskAssignmentRepo_Complete(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))
	assignment := newTaskAssignment(tenantA, task.ID, uuid.New())
	require.NoError(t, assignmentRepo.Create(ctx, assignment))

	completed, err := assignmentRepo.Complete(ctx, tenantA, assignment.ID, json.RawMessage(`{"outcome":"approved"}`))
	require.NoError(t, err)
	assert.False(t, completed.IsActive)
	assert.NotNil(t, completed.CompletedAt)
	assert.JSONEq(t, `{"outcome":"approved"}`, string(completed.ResultJSON))

	byTask, err := assignmentRepo.ListActiveByTask(ctx, tenantA, task.ID)
	require.NoError(t, err)
	assert.Empty(t, byTask, "a completed assignment is no longer active")
}

func TestTaskAssignmentRepo_SetLead(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, task))

	first := newTaskAssignment(tenantA, task.ID, uuid.New())
	first.IsLead = true
	require.NoError(t, assignmentRepo.Create(ctx, first))
	second := newTaskAssignment(tenantA, task.ID, uuid.New())
	require.NoError(t, assignmentRepo.Create(ctx, second))

	promoted, err := assignmentRepo.SetLead(ctx, tenantA, task.ID, second.ID)
	require.NoError(t, err)
	assert.True(t, promoted.IsLead)

	demoted, err := assignmentRepo.GetByID(ctx, tenantA, first.ID)
	require.NoError(t, err)
	assert.False(t, demoted.IsLead, "promoting a new lead must demote the previous one")
}
