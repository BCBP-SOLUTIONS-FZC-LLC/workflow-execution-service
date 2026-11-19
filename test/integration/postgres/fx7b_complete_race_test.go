//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestCompleteAssignment_TwoAssigneesCompleteTogether_TheLoserLosesOnTheVersionNotOnADeadlockAndOnlyTheWinnerAnswersAllDone(t *testing.T) {
	r := w25bNewRace(t)
	var outputs [2]port.CompleteAssignmentOutput
	complete := func(i int) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			out, err := r.deps.CompleteAssignment(ctx, port.CompleteAssignmentInput{
				TenantID: r.tenant.String(), TaskID: r.task.ID.String(), UserID: r.users[i].String(), ResultJSON: `{"ok":true}`,
			})
			outputs[i] = out
			return err
		}
	}

	results := r.runBoth(complete(0), complete(1))

	winner, loser := w25bSettle(t, results)
	assert.False(t, pgcommon.IsDeadlock(results[loser]), "the loser is not a deadlock victim: %v", results[loser])
	require.ErrorIs(t, results[loser], domain.ErrRecordVersionConflict)
	assert.True(t, outputs[winner].AllDone, "the winner closes the task")

	retry, err := r.deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: r.tenant.String(), TaskID: r.task.ID.String(), UserID: r.users[loser].String(), ResultJSON: `{"ok":true}`,
	})
	require.NoError(t, err)
	assert.False(t, retry.AllDone, "the loser's retry finds the task closed by the other assignee and does not close it again")

	again, err := r.deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: r.tenant.String(), TaskID: r.task.ID.String(), UserID: r.users[winner].String(), ResultJSON: `{"ok":true}`,
	})
	require.NoError(t, err)
	assert.True(t, again.AllDone, "a repeat of the winning call still answers that the task is done")
}
