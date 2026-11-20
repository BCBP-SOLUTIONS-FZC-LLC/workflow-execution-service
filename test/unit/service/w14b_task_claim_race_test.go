package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// w14bClaimRace lets another user's claim commit right after the claimant has
// listed the task's assignments. It answers with copies, so the claimant keeps
// the picture it read: no lead yet.
type w14bClaimRace struct {
	*csdAssignmentRepo
	afterList func(context.Context) error
	ran       bool
}

func (r *w14bClaimRace) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	rows, err := r.csdAssignmentRepo.ListActiveByTask(ctx, tenantID, taskID)
	out := make([]*domain.TaskAssignment, 0, len(rows))
	for _, row := range rows {
		snapshot := *row
		out = append(out, &snapshot)
	}
	if err == nil && r.afterList != nil && !r.ran {
		r.ran = true
		if hookErr := r.afterList(ctx); hookErr != nil {
			return nil, hookErr
		}
	}
	return out, err
}

func TestTaskServiceClaim_LosingTheRaceToAnotherClaimIsAVersionConflictNotAnAlreadyClaimedAnswer(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"})
	race := &w14bClaimRace{csdAssignmentRepo: h.assignments}
	race.afterList = func(ctx context.Context) error {
		_, err := h.assignments.ClaimLead(ctx, h.tenantID, h.task.ID, h.caller.ID, csdTaskVersion)
		return err
	}
	h.svc.Assignments = race

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.otherID, csdTaskVersion)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.NotErrorIs(t, err, port.ErrTaskAlreadyClaimed, "the loser had listed no lead yet, so the conflict comes from the write")
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID), "the first claim stands")
	assert.EqualValues(t, csdTaskVersion+1, h.task.RecordVersion, "only one claim bumped the task version")
	require.Len(t, h.assignments.claims, 1, "the loser's version is already stale at the version check, so it writes nothing")
}

func TestTaskServiceClaim_AClaimAfterTheLeadIsSetIsAlreadyClaimed(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"})
	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
	require.NoError(t, err)

	_, err = h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.otherID, csdTaskVersion+1)

	require.ErrorIs(t, err, port.ErrTaskAlreadyClaimed)
	assert.NotErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
	assert.Len(t, h.assignments.claims, 1, "the second claim never reached the write")
}
