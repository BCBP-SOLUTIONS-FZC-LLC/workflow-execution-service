package temporal_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fx8UserLookup struct{ tenantID, taskID, userID uuid.UUID }

// fx8LookupAssignments records the per-user lookups the completion makes.
type fx8LookupAssignments struct {
	*fakeAssignmentRepo
	lookups []fx8UserLookup
}

func (r *fx8LookupAssignments) ListByTaskAndUser(ctx context.Context, tenantID, taskID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lookups = append(r.lookups, fx8UserLookup{tenantID, taskID, userID})
	return r.fakeAssignmentRepo.ListByTaskAndUser(ctx, tenantID, taskID, userID)
}

func TestCompleteAssignment_ReassignedAssigneeWhoLosesTheRace_IsNotAllDone(t *testing.T) {
	tenantID, taskID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 2}
	winner := uuid.New()
	loser := uuid.New()
	finished := time.Now()

	// The winner's row completed; the loser's row came from a reassignment, so its id is not the
	// one derived from the task and user alone, and it was vacated with no result.
	winnerRow := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: winner}
	loserRow := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: loser}
	repo := newFakeAssignmentRepo(winnerRow, loserRow)
	winnerRow.IsActive, winnerRow.CompletedAt = false, &finished
	loserRow.IsActive = false
	assignments := &fx8LookupAssignments{fakeAssignmentRepo: repo}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), repo)
	deps.Assignments = assignments

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TaskID: taskID.String(), UserID: loser.String(), TenantID: tenantID.String(),
	})

	require.NoError(t, err)
	assert.False(t, out.AllDone, "the loser's call is not the one that closes the task")
	assert.Empty(t, outbox.enqueued)
	assert.Equal(t, []fx8UserLookup{{tenantID, taskID, loser}}, assignments.lookups, "the loser's rows are read by task and user in the caller's tenant")
}

func TestCompleteAssignment_ReassignedAssigneeWhoAlreadyCompleted_RetryIsAllDone(t *testing.T) {
	tenantID, taskID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 2}
	user := uuid.New()
	finished := time.Now()

	earlier := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: user}
	reassigned := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: user}
	repo := newFakeAssignmentRepo(earlier, reassigned)
	earlier.IsActive = false
	reassigned.IsActive, reassigned.CompletedAt = false, &finished
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), repo)

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TaskID: taskID.String(), UserID: user.String(), TenantID: tenantID.String(),
	})

	require.NoError(t, err)
	assert.True(t, out.AllDone, "a retry after the user's own completion still answers done")
}
