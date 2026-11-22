//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskAssignmentRepo_ListByTaskAndUser_AQueryThatFails_ReturnsTheDatabaseErrorUnmappedAndChangesNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()
	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	require.NoError(t, postgres.NewTaskRepo(appPool).Create(ctx, task))
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	held := newTaskAssignment(tenantID, task.ID, uuid.New())
	require.NoError(t, assignments.Create(ctx, held))

	var got []*domain.TaskAssignment
	err := postgres.NewTransactor(appPool).RunInTx(withGUC(ctx, tenantID), func(txCtx context.Context) error {
		cancelled, cancel := context.WithCancel(txCtx)
		cancel()
		var listErr error
		got, listErr = assignments.ListByTaskAndUser(cancelled, tenantID, task.ID, held.UserID)
		return listErr
	})

	require.ErrorIs(t, err, context.Canceled, "the driver's own error comes back")
	assert.NotErrorIs(t, err, domain.ErrNotFound, "a failed query is not an empty answer")
	assert.Nil(t, got, "no rows are handed back with the error")
	after, err := assignments.ListByTaskAndUser(ctx, tenantID, task.ID, held.UserID)
	require.NoError(t, err)
	require.Len(t, after, 1, "the failed read changed no row")
	assert.True(t, after[0].IsActive)
	assert.Equal(t, held.ID, after[0].ID)
}
