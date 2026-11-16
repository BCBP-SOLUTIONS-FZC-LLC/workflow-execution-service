package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestDelegationReassign_ANewDelegateWhoIsTheOldDelegateChangesNothing(t *testing.T) {
	for _, lead := range []bool{false, true} {
		name := "a non-lead holder"
		if lead {
			name = "the lead"
		}
		t.Run(name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			h.newDelegate = h.oldDelegate
			held := h.seed(t, "reviewer")
			held.assignment.IsLead = lead

			count, err := h.reassign()

			require.NoError(t, err, "a same-user move answers success, so a redelivered event is harmless")
			assert.Zero(t, count)
			assert.True(t, h.holdsActive(held.task.ID, h.oldDelegate), "the user keeps the task")
			assert.Equal(t, lead, held.assignment.IsLead)
			assert.Empty(t, h.assignments.created)
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, h.outbox.signals)
			assert.Zero(t, h.tx.commits)
		})
	}
}

func TestDelegationReroute_ADelegateWhoIsTheDelegatorChangesNothing(t *testing.T) {
	h := dlgfixNewRecon()
	h.delegate = h.delegator
	row := h.seed(t, "reviewer")

	require.NoError(t, h.reroute())

	assert.True(t, row.assignment.IsActive, "the delegator keeps the task")
	assert.Len(t, h.assignments.byID, 1)
	assert.Empty(t, h.outbox.enqueued)
	assert.Empty(t, h.outbox.signals)
	assert.Empty(t, h.temporal.signals)
}

func TestDelegationReverse_ADelegateWhoIsTheDelegatorChangesNothing(t *testing.T) {
	h := dlgfixNewRecon()
	h.delegate = h.delegator
	row := h.seed(t, "reviewer")
	delegationID := uuid.New()
	row.assignment.Reason = "delegation:" + delegationID.String()

	require.NoError(t, h.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate,
	}))

	assert.True(t, row.assignment.IsActive, "the user keeps the task")
	assert.Len(t, h.assignments.byID, 1)
	assert.Empty(t, h.outbox.enqueued)
	assert.Empty(t, h.outbox.signals)
	assert.Empty(t, h.temporal.signals)
}
