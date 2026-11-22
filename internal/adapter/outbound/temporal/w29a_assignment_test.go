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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w29aSceneWithNoActiveAssignee closes the scene's two assignments without a
// result, as when the task was finished by someone else and the others were
// vacated.
func w29aSceneWithNoActiveAssignee(t *testing.T) *w25bScene {
	t.Helper()
	s := w25bNewScene(t, true)
	vacated := w25bClock
	for _, a := range []*domain.TaskAssignment{s.lead, s.helper} {
		a.IsActive = false
		a.VacatedAt = &vacated
	}
	return s
}

// w29aAddOwnAssignment adds the assignment the plan names for the user on the
// scene's task: a row of the user's on the task under any id, closed, with or
// without a result.
func w29aAddOwnAssignment(s *w25bScene, userID uuid.UUID, withResult bool) *domain.TaskAssignment {
	closedAt := w25bClock
	a := &domain.TaskAssignment{
		ID: uuid.New(), TenantID: s.tenant, TaskID: s.task.ID, UserID: userID,
	}
	if withResult {
		a.CompletedAt = &closedAt
	} else {
		a.VacatedAt = &closedAt
	}
	s.rig.assignments[a.ID] = a
	s.rig.assignmentOrder = append(s.rig.assignmentOrder, a.ID)
	return a
}

func w29aCompleteAs(s *w25bScene, userID uuid.UUID) (port.CompleteAssignmentOutput, error) {
	return s.deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), UserID: userID.String(), ResultJSON: `{"late":true}`,
	})
}

func TestCompleteAssignment_NoActiveAssignment_AllDoneOnlyWhenTheUsersOwnAssignmentDidNotEndWithoutAResult(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		seed        func(s *w25bScene, user uuid.UUID)
		wantAllDone bool
	}{
		{
			name:        "the user's own assignment was closed by another assignee finishing first",
			seed:        func(s *w25bScene, user uuid.UUID) { w29aAddOwnAssignment(s, user, false) },
			wantAllDone: false,
		},
		{
			name:        "the user's own assignment holds their result: a retry after a lost answer",
			seed:        func(s *w25bScene, user uuid.UUID) { w29aAddOwnAssignment(s, user, true) },
			wantAllDone: true,
		},
		{
			name:        "the user has no assignment on the task at all",
			seed:        func(_ *w25bScene, _ uuid.UUID) {},
			wantAllDone: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w29aSceneWithNoActiveAssignee(t)
			user := uuid.New()
			tc.seed(s, user)
			before := s.rig.snapshot()

			out, err := w29aCompleteAs(s, user)

			require.NoError(t, err)
			assert.Equal(t, tc.wantAllDone, out.AllDone)
			assert.Empty(t, s.rig.callsTo("Assignments.Complete"), "no result is written")
			assert.Empty(t, s.rig.callsTo("Assignments.Vacate"))
			assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"), "no event is announced")
			assert.Empty(t, s.rig.outbox)
			w25bRequireUntouched(t, s.rig, before)
			assert.Equal(t, 1, s.rig.committed)
			assert.Equal(t, s.task.ID, s.rig.callsTo("Tasks.GetByID")[0].ID)
			s.rig.requireEveryCallScopedTo(t, s.tenant)
		})
	}
}

func TestCompleteAssignment_LookUpOfTheUsersOwnAssignmentFails_IsAWrappedRetryableErrorAndWritesNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cause error
	}{
		{name: "storage failure", cause: errW25bStorage},
		{name: "record version conflict", cause: domain.ErrRecordVersionConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w29aSceneWithNoActiveAssignee(t)
			user := uuid.New()
			w29aAddOwnAssignment(s, user, false)
			before := s.rig.snapshot()
			s.rig.failOn("Assignments.ListByTaskAndUser", tc.cause, 0)

			out, err := w29aCompleteAs(s, user)

			w25bRequireRetryable(t, err)
			var appErr *temporal.ApplicationError
			require.False(t, errors.As(err, &appErr), "a typed error would stop the retries")
			require.ErrorIs(t, err, tc.cause)
			assert.Contains(t, err.Error(), "list own assignments")
			assert.False(t, out.AllDone)
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Equal(t, 0, s.rig.committed)
			assert.Empty(t, s.rig.callsTo("Assignments.Complete"))
			assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"))
			w25bRequireUntouched(t, s.rig, before)

			delete(s.rig.failures, "Assignments.ListByTaskAndUser")
			again, err := w29aCompleteAs(s, user)
			require.NoError(t, err, "the retry finds the assignment")
			assert.False(t, again.AllDone)
		})
	}
}

func TestReassignAssignment_ToACoAssigneeWhoIsNotLead_LeadStaysWhereItIsUnlessTheNamedLeadHandsOver(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		leadless bool
		oldUser  string
	}{
		{
			name:    "the task has a lead and nobody is named",
			oldUser: "",
		},
		{
			name:    "the named old user holds nothing on the task",
			oldUser: uuid.NewString(),
		},
		{
			name:     "the task has no lead and the named old user holds nothing on the task",
			leadless: true,
			oldUser:  uuid.NewString(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			if tc.leadless {
				s.lead.IsLead = false
			}
			in := s.reassignInput(s.helperUser)
			in.OldUserID = tc.oldUser
			before := s.rig.snapshot()

			err := s.deps.ReassignAssignment(context.Background(), in)

			require.NoError(t, err)
			assert.Empty(t, s.rig.callsTo("Assignments.SetLead"), "the lead does not move")
			assert.Empty(t, s.rig.callsTo("Assignments.Vacate"))
			assert.Empty(t, s.rig.callsTo("Assignments.Create"))
			assert.Empty(t, s.rig.callsTo("Tasks.BumpRecordVersion"))
			assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"), "no reassignment is announced")
			w25bRequireUntouched(t, s.rig, before)
			assert.Equal(t, !tc.leadless, s.rig.assignments[s.lead.ID].IsLead)
			assert.False(t, s.rig.assignments[s.helper.ID].IsLead)
			assert.Equal(t, s.version, s.rig.tasks[s.task.ID].RecordVersion)
			s.rig.requireEveryCallScopedTo(t, s.tenant)
		})
	}
}
