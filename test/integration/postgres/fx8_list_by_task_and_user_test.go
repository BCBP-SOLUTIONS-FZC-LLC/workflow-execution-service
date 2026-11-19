//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskAssignmentRepo_ListByTaskAndUser_ReturnsEveryRowTheUserHoldsOrHeldOnThatTask(t *testing.T) {
	w := ivfixNewVacateWorld(t)
	ctx := context.Background()
	user := uuid.New()

	vacated := newTaskAssignment(w.tenantID, w.taskID, user)
	require.NoError(t, w.assignments.Create(ctx, vacated))
	_, err := w.assignments.Vacate(ctx, w.tenantID, vacated.ID)
	require.NoError(t, err)
	completed := newTaskAssignment(w.tenantID, w.taskID, user)
	require.NoError(t, w.assignments.Create(ctx, completed))
	_, err = w.assignments.Complete(ctx, w.tenantID, completed.ID, json.RawMessage(`{"outcome":"approved"}`), w.taskVersion)
	require.NoError(t, err)
	other := newTaskAssignment(w.tenantID, w.taskID, uuid.New())
	require.NoError(t, w.assignments.Create(ctx, other))

	got, err := w.assignments.ListByTaskAndUser(ctx, w.tenantID, w.taskID, user)

	require.NoError(t, err)
	ids := make([]uuid.UUID, len(got))
	for i, a := range got {
		ids[i] = a.ID
		assert.Equal(t, user, a.UserID)
		assert.Equal(t, w.taskID, a.TaskID)
	}
	assert.ElementsMatch(t, []uuid.UUID{vacated.ID, completed.ID}, ids, "closed rows are included and another user's row is not")
}

func TestTaskAssignmentRepo_ListByTaskAndUser_NoRowOrAnotherTenantIsAnEmptyList(t *testing.T) {
	w := ivfixNewVacateWorld(t)
	ctx := context.Background()
	held := w.seed(t)

	none, err := w.assignments.ListByTaskAndUser(ctx, w.tenantID, w.taskID, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, none)

	otherTenant, err := w.assignments.ListByTaskAndUser(ctx, uuid.New(), w.taskID, held.UserID)
	require.NoError(t, err)
	assert.Empty(t, otherTenant, "the tenant of the caller scopes the lookup")
}
