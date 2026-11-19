//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
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
