//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func w8bSeedTask(t *testing.T, pool *pgcommon.Pool, tenantID uuid.UUID) *domain.Task {
	t.Helper()
	ctx := context.Background()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(pool).Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, postgres.NewTaskRepo(pool).Create(ctx, task))
	return task
}

func w8bSeedAssignment(t *testing.T, pool *pgcommon.Pool, tenantID, userID uuid.UUID) (*domain.Task, *domain.TaskAssignment) {
	t.Helper()
	task := w8bSeedTask(t, pool, tenantID)
	assignment := newTaskAssignment(tenantID, task.ID, userID)
	require.NoError(t, postgres.NewTaskAssignmentRepo(pool).Create(context.Background(), assignment))
	return task, assignment
}

func TestTaskAssignmentRepo_ASharedUserInTwoTenants_IsOnlyReadAndVacatedInTheCallersTenant(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	cases := []struct {
		name string
		run  func(t *testing.T, tenantA, tenantB uuid.UUID, aAssignment, bAssignment *domain.TaskAssignment)
	}{
		{
			name: "listing a user's active assignments returns only the asked tenant's rows",
			run: func(t *testing.T, tenantA, tenantB uuid.UUID, aAssignment, bAssignment *domain.TaskAssignment) {
				got, err := assignments.ListActiveByUser(ctx, tenantA, aAssignment.UserID)
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, aAssignment.ID, got[0].ID)
				assert.Equal(t, tenantA, got[0].TenantID)

				got, err = assignments.ListActiveByUser(ctx, tenantB, bAssignment.UserID)
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, bAssignment.ID, got[0].ID)
				assert.Equal(t, tenantB, got[0].TenantID)
			},
		},
		{
			name: "listing filters by the tenant argument even when row security does not apply",
			run: func(t *testing.T, tenantA, tenantB uuid.UUID, aAssignment, bAssignment *domain.TaskAssignment) {
				unrestricted := postgres.NewTaskAssignmentRepo(superPool)
				got, err := unrestricted.ListActiveByUser(ctx, tenantA, aAssignment.UserID)
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, aAssignment.ID, got[0].ID)
			},
		},
		{
			name: "vacating the other tenant's assignment is not found and leaves it active",
			run: func(t *testing.T, tenantA, tenantB uuid.UUID, aAssignment, bAssignment *domain.TaskAssignment) {
				vacated, err := assignments.VacateActive(ctx, tenantA, bAssignment.ID)
				require.ErrorIs(t, err, domain.ErrNotFound)
				assert.Nil(t, vacated)

				stillActive, err := assignments.ListActiveByUser(ctx, tenantB, bAssignment.UserID)
				require.NoError(t, err)
				require.Len(t, stillActive, 1)
				assert.Equal(t, bAssignment.ID, stillActive[0].ID)
			},
		},
		{
			name: "vacating the caller's own assignment leaves the other tenant's active",
			run: func(t *testing.T, tenantA, tenantB uuid.UUID, aAssignment, bAssignment *domain.TaskAssignment) {
				vacated, err := assignments.VacateActive(ctx, tenantA, aAssignment.ID)
				require.NoError(t, err)
				require.NotNil(t, vacated)
				assert.Equal(t, aAssignment.ID, vacated.ID)

				mine, err := assignments.ListActiveByUser(ctx, tenantA, aAssignment.UserID)
				require.NoError(t, err)
				assert.Empty(t, mine)
				theirs, err := assignments.ListActiveByUser(ctx, tenantB, bAssignment.UserID)
				require.NoError(t, err)
				require.Len(t, theirs, 1)
				assert.Equal(t, bAssignment.ID, theirs[0].ID)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantA, tenantB, userID := uuid.New(), uuid.New(), uuid.New()
			_, aAssignment := w8bSeedAssignment(t, appPool, tenantA, userID)
			_, bAssignment := w8bSeedAssignment(t, appPool, tenantB, userID)
			tc.run(t, tenantA, tenantB, aAssignment, bAssignment)
		})
	}
}

func TestTaskAssignmentRepo_Create_ForATaskThatDoesNotExist_FailsWithoutAnAlreadyExistsAnswer(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	cases := []struct {
		name string
		make func(tenantID uuid.UUID) *domain.TaskAssignment
	}{
		{
			name: "no such task",
			make: func(tenantID uuid.UUID) *domain.TaskAssignment {
				return newTaskAssignment(tenantID, uuid.New(), uuid.New())
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			assignment := tc.make(tenantID)

			err := assignments.Create(ctx, assignment)

			require.Error(t, err)
			for _, answer := range []error{domain.ErrAlreadyExists, domain.ErrDuplicateActiveAssignment, domain.ErrNotFound} {
				assert.NotErrorIs(t, err, answer, "a database failure must not look like a client answer")
			}
			assert.Nil(t, assignment.AssignedAt, "the caller's value is not overwritten by a row that was never written")
			_, getErr := assignments.GetByID(ctx, tenantID, assignment.ID)
			assert.ErrorIs(t, getErr, domain.ErrNotFound, "nothing was stored")
		})
	}
}

func TestTaskAssignmentRepo_CountActiveByTasks_WhenTheQueryFails_ReturnsTheErrorAndNoCounts(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	assignments := postgres.NewTaskAssignmentRepo(appPool)

	tenantID := uuid.New()
	task, _ := w8bSeedAssignment(t, appPool, tenantID, uuid.New())

	healthy, err := assignments.CountActiveByTasks(context.Background(), tenantID, []uuid.UUID{task.ID})
	require.NoError(t, err)
	require.Equal(t, map[uuid.UUID]int{task.ID: 1}, healthy, "the same call counts the assignment when the query runs")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	counts, err := assignments.CountActiveByTasks(cancelled, tenantID, []uuid.UUID{task.ID})

	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, domain.ErrNotFound)
	assert.Empty(t, counts, "no partial counts accompany the error")
}

func TestTaskAssignmentRepo_LeadMoveOfAnUnknownTask_IsNotFoundAndStoresNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	cases := []struct {
		name string
		move func(tenantID, taskID, assignmentID uuid.UUID) (*domain.TaskAssignment, error)
	}{
		{
			name: "set lead",
			move: func(tenantID, taskID, assignmentID uuid.UUID) (*domain.TaskAssignment, error) {
				return assignments.SetLead(ctx, tenantID, taskID, assignmentID, 1)
			},
		},
		{
			name: "claim lead",
			move: func(tenantID, taskID, assignmentID uuid.UUID) (*domain.TaskAssignment, error) {
				return assignments.ClaimLead(ctx, tenantID, taskID, assignmentID, 1)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			task, assignment := w8bSeedAssignment(t, appPool, tenantID, uuid.New())
			require.False(t, assignment.IsLead)
			unknownTask := uuid.New()

			moved, err := tc.move(tenantID, unknownTask, assignment.ID)

			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.NotErrorIs(t, err, domain.ErrRecordVersionConflict)
			assert.Nil(t, moved)
			stored, getErr := assignments.GetByID(ctx, tenantID, assignment.ID)
			require.NoError(t, getErr)
			assert.False(t, stored.IsLead, "the real assignment was not promoted")
			storedTask, taskErr := postgres.NewTaskRepo(appPool).GetByID(ctx, tenantID, task.ID)
			require.NoError(t, taskErr)
			assert.Equal(t, task.RecordVersion, storedTask.RecordVersion, "no task version was bumped")
		})
	}
}
