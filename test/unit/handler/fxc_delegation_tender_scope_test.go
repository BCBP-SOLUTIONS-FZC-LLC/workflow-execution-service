package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestDelegationStarted_TenderScopeWithoutAnObjectIdIsRefusedAsMalformed(t *testing.T) {
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
			body := fxcDelegationStarted(eventID, "tender", scopeID)
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

func TestDelegationStarted_ScopesThatNameNoObjectOrNameOneAreAccepted(t *testing.T) {
	cases := []struct {
		scope   string
		scopeID any
	}{
		{"tender", "3f2504e0-4f89-41d3-9a0c-0305e82c3301"},
		{"all", nil},
	}
	for _, tc := range cases {
		t.Run(tc.scope, func(t *testing.T) {
			fakes := newEventsFakes()
			var calls []port.DelegationRerouteInput
			fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
				calls = append(calls, in)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))

			w := postEventTo(router, "/api/v1/internal/events/delegation", fxcDelegationStarted(uuid.New(), tc.scope, tc.scopeID))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, calls, 1)
			assert.Equal(t, tc.scope, calls[0].Scope)
		})
	}
}
