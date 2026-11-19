package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

var errFx10BumpDown = errors.New("task row write unavailable")

// fx10FailingBump fails the version bump of a task with a plain database error.
type fx10FailingBump struct {
	wkactTasks
}

func (r fx10FailingBump) BumpRecordVersion(context.Context, uuid.UUID, uuid.UUID, int64) (*domain.Task, error) {
	return nil, errFx10BumpDown
}

func (s *fx10HolderScene) requireNothingChanged(t *testing.T, committed int) {
	t.Helper()
	for _, a := range []*domain.TaskAssignment{s.lead, s.helper, s.other} {
		assert.True(t, s.world.assignments[a.ID].IsActive, "every assignee still holds the task")
	}
	assert.Empty(t, s.world.callsTo("Assignments.Vacate"), "nobody is vacated")
	assert.Empty(t, s.world.eventsOfType(domain.EventWorkflowTaskReassigned), "no move is recorded")
	assert.Equal(t, int64(4), s.task.RecordVersion, "the task version does not move")
	assert.Zero(t, committed, "nothing commits")
}

type fx10HolderScene struct{ *fx9HolderScene }

func fx10NewHolderScene() *fx10HolderScene { return &fx10HolderScene{fx9NewHolderScene()} }

func TestReassignAssignment_VacatingACoAssigneeWithAStaleVersion_IsANonRetryableConflictAndChangesNothing(t *testing.T) {
	t.Parallel()
	s := fx10NewHolderScene()
	deps, tx := wkactNewDeps(t, s.world)
	in := s.input(s.other.UserID, s.helper.UserID)
	in.RecordVersion = 3

	err := deps.ReassignAssignment(context.Background(), in)

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, "VersionConflict", appErr.Type())
	assert.True(t, appErr.NonRetryable(), "a lost version race is not worth a retry")
	s.requireNothingChanged(t, tx.committed)
	s.world.requireAllCallsForTenant(t, s.tenant)
}

func TestReassignAssignment_VacatingACoAssigneeWhenTheVersionBumpFails_IsRetryableAndChangesNothing(t *testing.T) {
	t.Parallel()
	s := fx10NewHolderScene()
	deps, tx := wkactNewDeps(t, s.world)
	deps.Tasks = fx10FailingBump{wkactTasks{w: s.world}}

	err := deps.ReassignAssignment(context.Background(), s.input(s.other.UserID, s.helper.UserID))

	require.ErrorIs(t, err, errFx10BumpDown, "the cause is kept")
	assert.ErrorContains(t, err, "bump task record version")
	assert.NotErrorIs(t, err, domain.ErrRecordVersionConflict)
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		assert.False(t, appErr.NonRetryable(), "a failed write is retried, not reported as a lost race")
	}
	s.requireNothingChanged(t, tx.committed)
}
