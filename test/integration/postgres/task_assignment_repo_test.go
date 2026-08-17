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

	completed, err := assignmentRepo.Complete(ctx, tenantA, assignment.ID, json.RawMessage(`{"outcome":"approved"}`), task.RecordVersion)
	require.NoError(t, err)
	assert.False(t, completed.IsActive)
	assert.NotNil(t, completed.CompletedAt)
	assert.JSONEq(t, `{"outcome":"approved"}`, string(completed.ResultJSON))

	byTask, err := assignmentRepo.ListActiveByTask(ctx, tenantA, task.ID)
	require.NoError(t, err)
	assert.Empty(t, byTask, "a completed assignment is no longer active")
}

// TestTaskAssignmentRepo_Complete_StaleTaskRecordVersion is the regression
// test for the missing-record-version-guard finding: Complete now bumps
// workflow_task.record_version as its optimistic-concurrency guard (the LLD
// frames the task, not the assignment, as claim/complete's contested
// resource), so a stale version must be rejected as a real conflict.
func TestTaskAssignmentRepo_Complete_StaleTaskRecordVersion(t *testing.T) {
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

	staleVersion := task.RecordVersion
	_, err := taskRepo.UpdateStatus(ctx, tenantA, task.ID, domain.TaskStatusInProgress, task.RecordVersion)
	require.NoError(t, err, "bump the task's own record_version out from under Complete's upcoming call")

	_, err = assignmentRepo.Complete(ctx, tenantA, assignment.ID, json.RawMessage(`{}`), staleVersion)
	assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
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

	promoted, err := assignmentRepo.SetLead(ctx, tenantA, task.ID, second.ID, task.RecordVersion)
	require.NoError(t, err)
	assert.True(t, promoted.IsLead)

	demoted, err := assignmentRepo.GetByID(ctx, tenantA, first.ID)
	require.NoError(t, err)
	assert.False(t, demoted.IsLead, "promoting a new lead must demote the previous one")
}

func TestTaskAssignmentRepo_ListActiveByUserPaginated(t *testing.T) {
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

	activeTask := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, activeTask))
	require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantA, activeTask.ID, userID)))

	// A vacated assignment for the same user must not appear.
	vacatedTask := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, vacatedTask))
	vacated := newTaskAssignment(tenantA, vacatedTask.ID, userID)
	require.NoError(t, assignmentRepo.Create(ctx, vacated))
	_, err := assignmentRepo.Vacate(ctx, tenantA, vacated.ID)
	require.NoError(t, err)

	rows, _, err := assignmentRepo.ListActiveByUserPaginated(ctx, tenantA, userID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, activeTask.ID, rows[0].TaskID)
	assert.Equal(t, activeTask.WorkflowInstanceID, rows[0].WorkflowInstanceID)
	assert.Equal(t, userID, rows[0].UserID)
	assert.Equal(t, domain.TaskStatusReady, rows[0].Status)
}

func TestTaskAssignmentRepo_ListActiveByUserPaginated_KeysetPagination(t *testing.T) {
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

	base := time.Now().UTC().Add(-time.Hour)
	var seeded []uuid.UUID
	for i := 0; i < 3; i++ {
		task := newTask(tenantA, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		seedTaskAt(t, superPool, ctx, task.ID, base.Add(time.Duration(i)*time.Minute))
		require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantA, task.ID, userID)))
		seeded = append(seeded, task.ID)
	}

	firstPage, next, err := assignmentRepo.ListActiveByUserPaginated(ctx, tenantA, userID, port.PageRequest{Limit: 2})
	require.NoError(t, err)
	require.Len(t, firstPage, 2)
	require.NotNil(t, next)
	assert.Equal(t, seeded[2], firstPage[0].TaskID)
	assert.Equal(t, seeded[1], firstPage[1].TaskID)

	secondPage, next2, err := assignmentRepo.ListActiveByUserPaginated(ctx, tenantA, userID, port.PageRequest{After: next, Limit: 2})
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	assert.Nil(t, next2)
	assert.Equal(t, seeded[0], secondPage[0].TaskID)
}

func TestTaskAssignmentRepo_VacateAllActiveByUser(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	userID := uuid.New()
	otherUserID := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))

	taskOne := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, taskOne))
	require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantA, taskOne.ID, userID)))

	taskTwo := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, taskTwo))
	require.NoError(t, assignmentRepo.Create(ctx, newTaskAssignment(tenantA, taskTwo.ID, userID)))

	// A different user's assignment must survive untouched.
	taskThree := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(ctx, taskThree))
	untouched := newTaskAssignment(tenantA, taskThree.ID, otherUserID)
	require.NoError(t, assignmentRepo.Create(ctx, untouched))

	vacated, err := assignmentRepo.VacateAllActiveByUser(ctx, tenantA, userID)
	require.NoError(t, err)
	assert.Len(t, vacated, 2)

	remaining, err := assignmentRepo.ListActiveByUser(ctx, tenantA, userID)
	require.NoError(t, err)
	assert.Empty(t, remaining)

	stillActive, err := assignmentRepo.GetByID(ctx, tenantA, untouched.ID)
	require.NoError(t, err)
	assert.True(t, stillActive.IsActive)
}
