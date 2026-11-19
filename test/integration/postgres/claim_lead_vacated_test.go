//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestClaimLead_AVacatedAssignmentIsRefusedAndNothingChanges(t *testing.T) {
	r := w25bNewRace(t)
	ctx := context.Background()
	_, err := r.assignments.VacateActive(ctx, r.tenant, r.held[0].ID)
	require.NoError(t, err)
	startVersion := r.task.RecordVersion

	_, err = r.assignments.ClaimLead(ctx, r.tenant, r.task.ID, r.held[0].ID, startVersion)

	require.ErrorIs(t, err, domain.ErrNotFound)
	task, err := r.tasks.GetByID(ctx, r.tenant, r.task.ID)
	require.NoError(t, err)
	assert.Equal(t, startVersion, task.RecordVersion, "the failed claim rolled back the version bump")
	vacated, err := r.assignments.GetByID(ctx, r.tenant, r.held[0].ID)
	require.NoError(t, err)
	assert.False(t, vacated.IsLead, "a closed assignment is never made lead")
}

func TestClaimAssignment_AVacatedAssignmentFailsWithoutRetryAndRecordsNoEvent(t *testing.T) {
	r := w25bNewRace(t)
	ctx := context.Background()
	_, err := r.assignments.VacateActive(ctx, r.tenant, r.held[0].ID)
	require.NoError(t, err)

	deps := *r.deps
	deps.Tasks = r.tasks

	err = deps.ClaimAssignment(ctx, port.ClaimAssignmentInput{
		TenantID: r.tenant.String(), AssignmentID: r.held[0].ID.String(), UserID: r.users[0].String(),
	})

	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "a closed assignment is not retried; got %v", err)
	assert.True(t, appErr.NonRetryable())
	assert.Empty(t, r.eventUsers(t, domain.EventWorkflowTaskClaimed, "claimed_by_user_id"))
}
