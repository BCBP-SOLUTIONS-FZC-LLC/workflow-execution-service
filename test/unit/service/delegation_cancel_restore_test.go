package service_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func dcrHop(delegationID, delegatorID uuid.UUID) string {
	return "delegation:" + delegationID.String() + ":" + delegatorID.String()
}

func TestDelegationCancel_ReturnsTheWorkToTheDelegatorItWasTakenFrom(t *testing.T) {
	t.Run("a named delegation", func(t *testing.T) {
		h := dlgfixNewClient(t)
		delegationID, delegator := uuid.New(), uuid.New()
		held := h.seed(t, "reviewer")
		held.assignment.Reason = dcrHop(delegationID, delegator)

		count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, h.holdsActive(held.task.ID, h.oldDelegate), "the delegate lost the task")
		assert.True(t, h.holdsActive(held.task.ID, delegator), "the original assignee holds it again")
		require.Len(t, h.outbox.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, h.outbox.signals[0].SignalName, "the workflow is told of a reassignment, not of an empty task")
		assert.Equal(t, csaReassignWire{TaskID: held.task.ID.String(), OldUserID: h.oldDelegate.String(), NewUserID: delegator.String()}, csaDecodeReassign(t, h.outbox.signals[0].Payload))
		require.Len(t, h.outbox.enqueued, 1)
		p := ivfixDecodeReassigned(t, h.outbox.enqueued[0])
		assert.Equal(t, delegator, p.NewUserID)
		require.NotNil(t, p.DelegationID)
		assert.Equal(t, delegationID, *p.DelegationID)
	})

	t.Run("no delegation named, a chained row goes back one hop and keeps the earlier chain", func(t *testing.T) {
		h := dlgfixNewClient(t)
		earlier, latest, earlierDelegator, delegator := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		held := h.seed(t, "reviewer")
		held.assignment.Reason = dcrHop(earlier, earlierDelegator) + ";" + dcrHop(latest, delegator)

		count, err := h.cancel()

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.True(t, h.holdsActive(held.task.ID, delegator))
		var restored *domain.TaskAssignment
		for _, a := range h.assignments.byID {
			if a.TaskID == held.task.ID && a.UserID == delegator && a.IsActive {
				restored = a
			}
		}
		require.NotNil(t, restored)
		assert.Equal(t, dcrHop(earlier, earlierDelegator), restored.Reason)
	})

	t.Run("a delegator who is no longer eligible leaves the task without an assignee, as before", func(t *testing.T) {
		h := dlgfixNewClient(t)
		h.withEligibility(func(string) (bool, error) { return false, nil })
		delegationID, delegator := uuid.New(), uuid.New()
		held := h.seed(t, "reviewer")
		held.assignment.Reason = dcrHop(delegationID, delegator)

		count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, h.holdsActive(held.task.ID, delegator))
		require.Len(t, h.outbox.signals, 1)
		assert.Equal(t, port.SignalInstanceTaskVacated, h.outbox.signals[0].SignalName)
	})
}
