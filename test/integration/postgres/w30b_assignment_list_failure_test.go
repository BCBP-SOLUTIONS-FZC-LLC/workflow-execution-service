//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func w30bRequireFailedLookup(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	for _, answer := range []error{
		domain.ErrNotFound, domain.ErrRecordVersionConflict, domain.ErrAlreadyExists,
		domain.ErrDuplicateActiveAssignment, port.ErrTenantRetentionChanged,
	} {
		assert.NotErrorIs(t, err, answer, "a failed lookup must not look like an empty or a client answer")
	}
}

func TestTaskAssignmentRepo_ListByTaskAndUser_QueryFails_ReturnsTheErrorAndNoSlice(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	tenantID, taskID, userID := uuid.New(), uuid.New(), uuid.New()

	appPool := newAppRolePool(t, superPool, superDSN)
	assignments := postgres.NewTaskAssignmentRepo(appPool)

	t.Run("statement fails", func(t *testing.T) {
		empty, err := assignments.ListByTaskAndUser(ctx, tenantID, taskID, userID)
		require.NoError(t, err, "before the failure the same call answers an empty list")
		require.NotNil(t, empty)
		require.Empty(t, empty)
		dropTable(t, superPool, "workflow_task_assignment")

		got, err := assignments.ListByTaskAndUser(ctx, tenantID, taskID, userID)

		w30bRequireFailedLookup(t, err)
		assert.Nil(t, got, "no slice, not even an empty one, comes back with the error")
	})

	t.Run("pool closed", func(t *testing.T) {
		appPool.Close()

		got, err := assignments.ListByTaskAndUser(ctx, tenantID, taskID, userID)

		w30bRequireFailedLookup(t, err)
		assert.Nil(t, got)
	})
}
