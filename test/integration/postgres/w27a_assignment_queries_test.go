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

func w27aAssignmentIDs(rows []*domain.TaskAssignment) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	return ids
}

func TestTaskAssignmentRepo_ListActiveByUser_LeavesOutCompletedAndVacatedRowsAndKeepsTheCompletedResult(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenant, otherTenant := uuid.New(), uuid.New()
	user := uuid.New()
	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	seed := func() (*domain.Task, *domain.TaskAssignment) {
		task := newTask(tenant, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		a := newTaskAssignment(tenant, task.ID, user)
		require.NoError(t, assignmentRepo.Create(ctx, a))
		return task, a
	}
	_, active := seed()
	completedTask, completed := seed()
	_, vacated := seed()
	_, err := assignmentRepo.Complete(ctx, tenant, completed.ID, json.RawMessage(`{"outcome":"approved"}`), completedTask.RecordVersion)
	require.NoError(t, err)
	_, err = assignmentRepo.Vacate(ctx, tenant, vacated.ID)
	require.NoError(t, err)

	got, err := assignmentRepo.ListActiveByUser(ctx, tenant, user)

	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{active.ID}, w27aAssignmentIDs(got), "only the assignment still open is listed; completed and vacated rows are not counted")
	kept, err := assignmentRepo.GetByID(ctx, tenant, completed.ID)
	require.NoError(t, err)
	assert.False(t, kept.IsActive)
	assert.NotNil(t, kept.CompletedAt, "the completed row keeps its completion time")
	assert.JSONEq(t, `{"outcome":"approved"}`, string(kept.ResultJSON), "and its result")
	assert.Nil(t, kept.VacatedAt, "a completed row was not vacated")
	crossTenant, err := assignmentRepo.ListActiveByUser(ctx, otherTenant, user)
	require.NoError(t, err)
	assert.Empty(t, crossTenant, "another tenant's id on the same pool sees none of these rows")
}

func TestTaskAssignmentRepo_VacateAllActiveByUser_VacatesEveryActiveRowOfTheUserInTheTenantWithNoScopeFilter(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	tenant, otherTenant := uuid.New(), uuid.New()
	user, otherUser := uuid.New(), uuid.New()
	seed := func(tenantID, instanceID, userID uuid.UUID, reason string, lead bool) *domain.TaskAssignment {
		task := newTask(tenantID, instanceID)
		require.NoError(t, taskRepo.Create(ctx, task))
		a := newTaskAssignment(tenantID, task.ID, userID)
		a.Reason = reason
		a.IsLead = lead
		require.NoError(t, assignmentRepo.Create(ctx, a))
		return a
	}
	instOne := newInstance(tenant, time.Now().UTC())
	instTwo := newInstance(tenant, time.Now().UTC())
	instOther := newInstance(otherTenant, time.Now().UTC())
	for _, inst := range []*domain.Instance{instOne, instTwo, instOther} {
		require.NoError(t, instanceRepo.Create(ctx, inst))
	}
	delegationTag := "delegation:" + uuid.NewString() + ":" + uuid.NewString()
	plain := seed(tenant, instOne.ID, user, "", true)
	tagged := seed(tenant, instOne.ID, user, delegationTag, false)
	onSecondInstance := seed(tenant, instTwo.ID, user, "", false)
	taggedSecond := seed(tenant, instTwo.ID, user, delegationTag, true)
	wantVacated := []uuid.UUID{plain.ID, tagged.ID, onSecondInstance.ID, taggedSecond.ID}
	completedRow := seed(tenant, instTwo.ID, user, "", false)
	completedTask, err := taskRepo.GetByID(ctx, tenant, completedRow.TaskID)
	require.NoError(t, err)
	_, err = assignmentRepo.Complete(ctx, tenant, completedRow.ID, json.RawMessage(`{"outcome":"done"}`), completedTask.RecordVersion)
	require.NoError(t, err)
	earlier := seed(tenant, instOne.ID, user, "", false)
	earlierVacated, err := assignmentRepo.Vacate(ctx, tenant, earlier.ID)
	require.NoError(t, err)
	anotherUsers := seed(tenant, instOne.ID, otherUser, "", false)
	sameUserOtherTenant := seed(otherTenant, instOther.ID, user, "", false)

	vacatedAsOtherTenant, err := assignmentRepo.VacateAllActiveByUser(ctx, otherTenant, user)

	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{sameUserOtherTenant.ID}, w27aAssignmentIDs(vacatedAsOtherTenant), "the call is bound to its own tenant")
	for _, id := range wantVacated {
		row, err := assignmentRepo.GetByID(ctx, tenant, id)
		require.NoError(t, err)
		assert.True(t, row.IsActive, "the other tenant's call left this tenant's row active")
	}

	vacated, err := assignmentRepo.VacateAllActiveByUser(ctx, tenant, user)

	require.NoError(t, err)
	assert.ElementsMatch(t, wantVacated, w27aAssignmentIDs(vacated), "rows on both instances, the lead rows and both delegation-tagged rows")
	for _, row := range vacated {
		assert.Equal(t, tenant, row.TenantID)
		assert.False(t, row.IsActive)
		assert.NotNil(t, row.VacatedAt)
	}
	for _, row := range vacated {
		if row.ID == tagged.ID || row.ID == taggedSecond.ID {
			assert.Equal(t, delegationTag, row.Reason, "the delegation tag is kept on the vacated row")
		}
	}
	remaining, err := assignmentRepo.ListActiveByUser(ctx, tenant, user)
	require.NoError(t, err)
	assert.Empty(t, remaining)
	untouched, err := assignmentRepo.GetByID(ctx, tenant, anotherUsers.ID)
	require.NoError(t, err)
	assert.True(t, untouched.IsActive, "another user's row stays active")
	stillCompleted, err := assignmentRepo.GetByID(ctx, tenant, completedRow.ID)
	require.NoError(t, err)
	assert.NotNil(t, stillCompleted.CompletedAt)
	assert.Nil(t, stillCompleted.VacatedAt, "a completed row is not vacated")
	stillVacated, err := assignmentRepo.GetByID(ctx, tenant, earlier.ID)
	require.NoError(t, err)
	require.NotNil(t, stillVacated.VacatedAt)
	assert.True(t, earlierVacated.VacatedAt.Equal(*stillVacated.VacatedAt), "an earlier vacate time is not overwritten")
}
