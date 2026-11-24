package handler_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func fxcDelegationStarted(eventID uuid.UUID, scope string, scopeID any) map[string]any {
	payload := map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID, "delegate_user_id": testDelegateID2,
		"scope": scope, "starts_at": time.Now().Format(time.RFC3339),
	}
	if scopeID != nil {
		payload["scope_id"] = scopeID
	}
	return envelope("DelegationStarted", eventID, testTenantID, time.Now(), payload)
}

func TestDelegationStarted_DepartmentScopeWithoutADepartmentIdIsRefusedAsMalformed(t *testing.T) {
	cases := map[string]any{
		"scope_id left out":      nil,
		"scope_id explicit null": "null",
		"scope_id empty":         "",
	}
	for name, scopeID := range cases {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			var calls []port.DelegationRerouteInput
			fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
				calls = append(calls, in)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			eventID := uuid.New()
			body := fxcDelegationStarted(eventID, "department", scopeID)
			if scopeID == "null" {
				body["data"].(map[string]any)["scope_id"] = nil
			}

			w := postEventTo(router, "/api/v1/internal/events/delegation", body)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Equal(t, "BAD_REQUEST", w12aDecodeProblem(t, w).Code)
			assert.Empty(t, calls, "no work is moved")
			processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
			require.NoError(t, err)
			assert.False(t, processed, "the event is not marked processed")
		})
	}
}

func TestDelegationStarted_ADepartmentScopeWithADepartmentIdIsAccepted(t *testing.T) {
	fakes := newEventsFakes()
	var calls []port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
		calls = append(calls, in)
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))
	departmentID := uuid.NewString()

	w := postEventTo(router, "/api/v1/internal/events/delegation", fxcDelegationStarted(uuid.New(), "department", departmentID))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, calls, 1)
	assert.Equal(t, testTenantID, calls[0].TenantID)
	assert.Equal(t, "department", calls[0].Scope)
	require.NotNil(t, calls[0].ScopeID)
	assert.Equal(t, departmentID, *calls[0].ScopeID)
}
