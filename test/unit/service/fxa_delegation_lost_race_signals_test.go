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

func TestDelegationReroute_NoResumeIsRecordedWhenEveryRowLostTheRace(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	row := h.seed(t, "reviewer")
	row.instance.Status = domain.InstanceStatusPaused
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
	tx.onStart = func() { row.assignment.IsActive = false }

	require.NoError(t, h.reroute())

	assert.Empty(t, h.outbox.signals, "no move, so no resume and no reassign signal")
	assert.Empty(t, h.outbox.enqueued)
}

func TestDelegationReroute_TheResumeIsRecordedOnlyForAnInstanceWhoseRowMoved(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	lost, moved := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	lost.instance.Status = domain.InstanceStatusPaused
	moved.instance.Status = domain.InstanceStatusPaused
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
	tx.onStart = func() { lost.assignment.IsActive = false }

	require.NoError(t, h.reroute())

	assert.Zero(t, dlgfixResumeCount(h, lost.instance.ID))
	assert.Equal(t, 1, dlgfixResumeCount(h, moved.instance.ID))
}

func TestDelegationReverse_NoPauseIsRecordedWhenEveryRowLostTheRace(t *testing.T) {
	h := dlgfixNewRecon()
	tx, _ := h.transactional()
	delegationID := uuid.New()
	row := h.seed(t, "reviewer")
	row.assignment.UserID = h.delegate
	row.assignment.Reason = "delegation:" + delegationID.String()
	row.instance.Status = domain.InstanceStatusRunning
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
	tx.onStart = func() { row.assignment.IsActive = false }

	require.NoError(t, h.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
	}))

	assert.Empty(t, h.outbox.signals, "no move, so no pause and no reassign signal")
}
