package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fx10Unwired builds a delegate client missing the collaborators the eligibility check needs.
func fx10Unwired(t *testing.T, w fx9Wiring) *dlgfixClient {
	t.Helper()
	h := dlgfixNewClient(t)
	h.withEligibility(func(string) (bool, error) { return true, nil })
	if !w.withEligibility {
		h.svc.Eligibility = nil
	}
	if !w.withDefinitions {
		h.svc.Definitions = nil
	}
	return h
}

func TestWorkflowClient_ReassignDelegate_BuiltWithoutWhatTheEligibilityCheckNeeds_RefusesAndMovesNothing(t *testing.T) {
	for _, w := range fx9IncompleteWirings() {
		t.Run(w.name, func(t *testing.T) {
			h := fx10Unwired(t, w)
			held := h.seed(t, "reviewer")

			count, err := h.reassign()

			require.Error(t, err)
			assert.ErrorContains(t, err, "eligibility checker", "the refusal names what is missing")
			assert.Zero(t, count)
			h.requireUntouched(t, held)
			assert.Zero(t, h.tx.commits, "nothing is written for an unchecked delegate")
		})
	}
}

func TestWorkflowClient_CancelByDelegate_ReturnToDelegatorBuiltWithoutWhatTheEligibilityCheckNeeds_RefusesAndVacatesNothing(t *testing.T) {
	for _, w := range fx9IncompleteWirings() {
		t.Run(w.name, func(t *testing.T) {
			h := fx10Unwired(t, w)
			delegationID, delegator := uuid.New(), uuid.New()
			held := h.seed(t, "reviewer")
			held.assignment.Reason = dcrHop(delegationID, delegator)

			count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: h.tenantID, DelegateUserID: h.oldDelegate, DelegationID: &delegationID})

			require.Error(t, err)
			assert.ErrorContains(t, err, "eligibility checker", "the refusal names what is missing")
			assert.Zero(t, count)
			assert.True(t, h.holdsActive(held.task.ID, h.oldDelegate), "the delegate keeps the task")
			assert.False(t, h.holdsActive(held.task.ID, delegator), "an unchecked delegator is not given the task back")
			assert.Empty(t, h.outbox.signals)
		})
	}
}
