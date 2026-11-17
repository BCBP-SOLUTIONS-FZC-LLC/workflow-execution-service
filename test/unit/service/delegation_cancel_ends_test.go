package service_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestDelegationCancel_RecordsTheNamedDelegationAsEnded(t *testing.T) {
	t.Run("inside the transaction that cancels the work", func(t *testing.T) {
		h := dlgfixNewClient(t)
		ended := newFakeEndedDelegations()
		h.svc.Ended = ended
		delegationID := uuid.New()
		held := h.seed(t, "reviewer")
		held.assignment.Reason = "delegation:" + delegationID.String()
		var running []bool
		ended.onMark = func() { running = append(running, h.tx.running) }

		count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.marked)
		assert.Equal(t, []bool{true}, running, "the end is recorded with the cancel, not apart from it")
	})

	t.Run("also when the delegate holds nothing of it", func(t *testing.T) {
		h := dlgfixNewClient(t)
		ended := newFakeEndedDelegations()
		h.svc.Ended = ended
		delegationID := uuid.New()

		count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

		require.NoError(t, err)
		assert.Zero(t, count)
		assert.Equal(t, []endedDelegationKey{{h.tenantID, delegationID}}, ended.marked)
	})

	t.Run("not when no delegation is named", func(t *testing.T) {
		h := dlgfixNewClient(t)
		ended := newFakeEndedDelegations()
		h.svc.Ended = ended
		h.seed(t, "reviewer")

		_, err := h.cancel()

		require.NoError(t, err)
		assert.Empty(t, ended.marked)
	})

	t.Run("a record that cannot be written fails the cancel and vacates nothing", func(t *testing.T) {
		h := dlgfixNewClient(t)
		ended := newFakeEndedDelegations()
		ended.markErr = assert.AnError
		h.svc.Ended = ended
		delegationID := uuid.New()
		held := h.seed(t, "reviewer")
		held.assignment.Reason = "delegation:" + delegationID.String()

		count, err := h.svc.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

		require.ErrorIs(t, err, assert.AnError)
		assert.Zero(t, count)
		assert.True(t, held.assignment.IsActive)
	})
}

func TestDelegationStarted_AfterAnHTTPCancelOfTheSameDelegationMovesNothing(t *testing.T) {
	w := newEndedFirstWorld(t)
	delegationID := uuid.New()
	require.NoError(t, w.start(delegationID))
	client := &service.WorkflowClient{
		Instances: w.svc.Instances, Tasks: w.svc.Tasks, Assignments: w.svc.Assignments, Transactor: w.svc.Transactor,
		Outbox: w.svc.Outbox, Signals: w.svc.Signals, Validator: w.svc.Validator, Ended: w.ended,
	}
	cancelled, err := client.CancelByDelegate(t.Context(), port.CancelByDelegateInput{TenantID: w.tenantID, DelegateUserID: w.delegate, DelegationID: &delegationID})
	require.NoError(t, err)
	require.Equal(t, 1, cancelled)
	require.NotNil(t, assignmentOf(w.assignments, w.task.ID, w.delegator), "the cancel returned the work")

	require.NoError(t, w.start(delegationID), "a late redelivery of the start")

	assert.Nil(t, assignmentOf(w.assignments, w.task.ID, w.delegate), "the cancelled delegation does not re-delegate the work")
	assert.NotNil(t, assignmentOf(w.assignments, w.task.ID, w.delegator))
}
