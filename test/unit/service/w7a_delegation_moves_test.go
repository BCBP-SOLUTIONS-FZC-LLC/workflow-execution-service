package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w7aEligibilityCall struct {
	tenantID     uuid.UUID
	userID       uuid.UUID
	departmentID uuid.UUID
	level        string
}

// w7aRecordEligibility turns the eligibility check on, answers yes, and records
// every question asked.
func w7aRecordEligibility(h *dlgfixClient) *[]w7aEligibilityCall {
	calls := &[]w7aEligibilityCall{}
	h.svc.Definitions = h.definitions
	h.svc.Eligibility = &fakeEligibilityChecker{check: func(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
		*calls = append(*calls, w7aEligibilityCall{tenantID: tenantID, userID: userID, departmentID: departmentID, level: level})
		return true, nil
	}}
	return calls
}

func TestDelegationReassign_ReadOfActiveAssignmentsFailsInTransaction_RollsBackEarlierMoves(t *testing.T) {
	cases := []struct {
		name string
		// okReads is how many reads of a task's active assignments succeed
		// before the next one fails.
		okReads int
	}{
		{name: "the second row's re-read fails after the first row moved", okReads: 1},
		{name: "the first row's re-read fails", okReads: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			reads := 0
			h.assignments.afterList = func(uuid.UUID) {
				reads++
				if reads == tc.okReads {
					h.assignments.listActiveByTaskErr = assert.AnError
				}
			}
			if tc.okReads == 0 {
				h.assignments.listActiveByTaskErr = assert.AnError
			}

			count, err := h.reassign()

			require.ErrorIs(t, err, assert.AnError)
			assert.Zero(t, count, "nothing is counted")
			h.requireUntouched(t, first, second)
			assert.Equal(t, 1, h.tx.rollbacks, "the failure leaves the transaction callback, so it rolls back")
			assert.Zero(t, h.tx.commits)
			assert.Empty(t, h.outbox.enqueued, "no reassigned event outlives the rollback")
			assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants)
		})
	}
}

func TestDelegationCancel_HopWithNoRestorableDelegator_OnlyVacatesTheRow(t *testing.T) {
	cases := []struct {
		name   string
		reason func(h *dlgfixClient, delegationID uuid.UUID) string
	}{
		{
			name: "the hop was written without a delegator id",
			reason: func(_ *dlgfixClient, delegationID uuid.UUID) string {
				return "delegation:" + delegationID.String()
			},
		},
		{
			name: "the hop's delegator id is not readable",
			reason: func(_ *dlgfixClient, delegationID uuid.UUID) string {
				return "delegation:" + delegationID.String() + ":not-a-user-id"
			},
		},
		{
			name: "the delegator is the holder itself",
			reason: func(h *dlgfixClient, delegationID uuid.UUID) string {
				return dcrHop(delegationID, h.oldDelegate)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			calls := w7aRecordEligibility(h)
			delegationID := uuid.New()
			held := h.seed(t, "reviewer")
			held.assignment.Reason = tc.reason(h, delegationID)

			count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{
				TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID,
			})

			require.NoError(t, err)
			assert.Equal(t, 1, count)
			assert.False(t, held.assignment.IsActive, "the row is vacated")
			assert.Empty(t, h.assignments.created, "no assignment is created for anyone")
			assert.False(t, h.holdsActive(held.task.ID, h.oldDelegate))
			assert.Empty(t, *calls, "nobody is asked about eligibility, there is no one to restore")
			assert.Empty(t, h.outbox.enqueued, "no reassigned event is recorded")
			require.Len(t, h.outbox.signals, 1)
			assert.Equal(t, port.SignalInstanceTaskVacated, h.outbox.signals[0].SignalName,
				"the workflow is told the task has no assignee, not that it was reassigned")
			assert.Equal(t, held.instance.ID, h.outbox.signals[0].InstanceID)
		})
	}
}

func TestDelegationCancel_RowsReturnToTheSameDelegator_AsksEligibilityOncePerDepartmentAndLevel(t *testing.T) {
	cases := []struct {
		name      string
		levels    []string
		wantCalls []string
	}{
		{name: "two rows with the same level", levels: []string{"reviewer", "reviewer"}, wantCalls: []string{"reviewer"}},
		{name: "a third row with another level asks again", levels: []string{"reviewer", "reviewer", "approver"}, wantCalls: []string{"reviewer", "approver"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			calls := w7aRecordEligibility(h)
			delegator := uuid.New()
			var held []dlgfixHeld
			for _, level := range tc.levels {
				row := h.seed(t, level)
				row.assignment.Reason = dcrHop(uuid.New(), delegator)
				held = append(held, row)
			}

			count, err := h.cancel()

			require.NoError(t, err)
			assert.Equal(t, len(tc.levels), count)
			require.Len(t, *calls, len(tc.wantCalls), "one membership call per distinct department and level")
			for i, level := range tc.wantCalls {
				assert.Equal(t, w7aEligibilityCall{tenantID: h.tenantID, userID: delegator, departmentID: held[0].task.DepartmentID, level: level}, (*calls)[i])
			}
			for _, row := range held {
				assert.True(t, h.holdsActive(row.task.ID, delegator), "the answer is reused for every row")
				assert.False(t, h.holdsActive(row.task.ID, h.oldDelegate))
			}
			assert.Len(t, h.outbox.signals, len(tc.levels))
			for _, sig := range h.outbox.signals {
				assert.Equal(t, port.SignalInstanceReassign, sig.SignalName)
			}
		})
	}
}
