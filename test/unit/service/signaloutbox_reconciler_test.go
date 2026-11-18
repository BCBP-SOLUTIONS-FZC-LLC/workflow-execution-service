package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestDelegationReroute_RecordsTheReassignSignalInTheTransactionAndSendsNothingDirectly(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	row := h.seed(t, "reviewer")
	var runningAtRecord []bool
	var commitsAtRecord []int
	h.outbox.onSignal = func() {
		runningAtRecord = append(runningAtRecord, tx.running)
		commitsAtRecord = append(commitsAtRecord, tx.commits)
	}

	require.NoError(t, h.reroute())

	require.Len(t, h.outbox.signals, 1)
	sent := h.outbox.signals[0]
	assert.Equal(t, port.SignalInstanceReassign, sent.SignalName)
	assert.Equal(t, row.instance.ID, sent.InstanceID)
	assert.Equal(t, row.instance.TemporalWorkflowID, sent.TemporalWorkflowID)
	assert.Equal(t, csaReassignWire{TaskID: row.task.ID.String(), OldUserID: h.delegator.String(), NewUserID: h.delegate.String()}, csaDecodeReassign(t, sent.Payload))
	assert.False(t, recordedAtOf(t, sent.Payload).IsZero(), "the signal carries the time it was recorded, because delivery is not ordered")
	assert.Equal(t, []uuid.UUID{h.tenantID}, h.outbox.signalTenants)
	assert.Equal(t, []bool{true}, runningAtRecord, "recorded while the transaction is open")
	assert.Equal(t, []int{0}, commitsAtRecord, "before the commit, so a rollback takes it away")
	assert.Empty(t, h.temporal.signals, "the delivery is the relay's job, not a direct call after the commit")
}

func TestDelegationReroute_ASignalThatCannotBeRecordedFailsTheEventAndChangesNothing(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.outbox.signalErr = assert.AnError

	require.ErrorIs(t, h.reroute(), assert.AnError, "the event is redelivered and the whole move tried again")

	h.requireNothingMoved(t, first, second)
	assert.Equal(t, 1, tx.rollbacks)
}

func TestDelegationReverse_RecordsTheReassignSignalThatHandsTheTaskBack(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	delegationID := uuid.New()
	row := h.seed(t, "reviewer")
	row.assignment.UserID = h.delegate
	row.assignment.Reason = "delegation:" + delegationID.String()
	var runningAtRecord []bool
	h.outbox.onSignal = func() { runningAtRecord = append(runningAtRecord, tx.running) }

	require.NoError(t, h.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate,
	}))

	reassigns := h.outbox.signalsNamed(port.SignalInstanceReassign)
	require.Len(t, reassigns, 1)
	assert.Equal(t, row.instance.ID, reassigns[0].InstanceID)
	assert.Equal(t, csaReassignWire{TaskID: row.task.ID.String(), OldUserID: h.delegate.String(), NewUserID: h.delegator.String()}, csaDecodeReassign(t, reassigns[0].Payload))
	assert.Equal(t, []bool{true, true}, runningAtRecord, "the resume for the available delegator and the reassign are both recorded inside the transaction")
	assert.Empty(t, h.temporal.signalsNamed(port.SignalInstanceReassign), "no direct reassign signal")
}

func TestDelegationReroute_ARowThatMovedIsTheOnlyOneSignalled(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	ended, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	tx.onStart = func() { ended.assignment.IsActive = false }

	require.NoError(t, h.reroute())

	require.Len(t, h.outbox.signals, 1)
	assert.Equal(t, live.instance.ID, h.outbox.signals[0].InstanceID)
}
