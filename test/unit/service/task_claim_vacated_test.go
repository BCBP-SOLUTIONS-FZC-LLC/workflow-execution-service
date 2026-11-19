package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type tcvVacatingList struct {
	*csdAssignmentRepo
	vacate *domain.TaskAssignment
}

func (r *tcvVacatingList) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	listed, err := r.csdAssignmentRepo.ListActiveByTask(ctx, tenantID, taskID)
	snapshot := make([]*domain.TaskAssignment, len(listed))
	for i, a := range listed {
		copied := *a
		snapshot[i] = &copied
	}
	r.vacate.IsActive = false
	return snapshot, err
}

func TestTaskServiceClaim_AnAssignmentVacatedAfterTheReadRefusesTheClaimAndEmitsNoEvent(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	h.svc.Assignments = &tcvVacatingList{csdAssignmentRepo: h.assignments, vacate: h.caller}

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrNotAssignee)
	assert.Empty(t, h.outbox.enqueued, "no claimed event for a user who no longer holds the task")
	assert.Equal(t, uuid.Nil, h.assignments.leadOf(h.task.ID))
}
