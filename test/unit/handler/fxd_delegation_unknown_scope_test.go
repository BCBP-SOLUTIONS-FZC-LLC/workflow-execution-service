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

func TestDelegationStarted_ScopeOutsideTheKnownListIsRefusedAsMalformed(t *testing.T) {
	cases := map[string]string{
		"unknown word":    "region",
		"empty":           "",
		"wrong case":      "ALL",
		"padded":          " tender",
		"plural of known": "departments",
	}
	for name, scope := range cases {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			var calls []port.DelegationRerouteInput
			fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
				calls = append(calls, in)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			eventID := uuid.New()

			w := postEventTo(router, "/api/v1/internal/events/delegation", fxcDelegationStarted(eventID, scope, uuid.NewString()))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Equal(t, "BAD_REQUEST", w12aDecodeProblem(t, w).Code)
			assert.Empty(t, calls, "no work is moved")
			processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
			require.NoError(t, err)
			assert.False(t, processed, "the event is not marked processed")
		})
	}
}

func TestDelegationStarted_ObjectScopeWithAScopeIdThatIsNotAUUIDIsRefusedAsMalformed(t *testing.T) {
	scopeIDs := map[string]any{
		"missing":         nil,
		"empty":           "",
		"plain text":      "TENDER-42",
		"truncated uuid":  "3f2504e0-4f89-41d3-9a0c",
		"uuid and suffix": uuid.NewString() + "x",
	}
	for _, scope := range []string{"department", "tender"} {
		for name, scopeID := range scopeIDs {
			t.Run(scope+" "+name, func(t *testing.T) {
				fakes := newEventsFakes()
				var calls []port.DelegationRerouteInput
				fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
					calls = append(calls, in)
					return nil
				}
				router := newInternalRouter(newEventsHandler(fakes))
				eventID := uuid.New()

				w := postEventTo(router, "/api/v1/internal/events/delegation", fxcDelegationStarted(eventID, scope, scopeID))

				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				assert.Equal(t, "BAD_REQUEST", w12aDecodeProblem(t, w).Code)
				assert.Empty(t, calls, "no work is moved")
				processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
				require.NoError(t, err)
				assert.False(t, processed, "the event is not marked processed")
			})
		}
	}
}

func TestDelegationStarted_AllScopeIgnoresAScopeIdThatIsNotAUUID(t *testing.T) {
	fakes := newEventsFakes()
	var calls []port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
		calls = append(calls, in)
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEventTo(router, "/api/v1/internal/events/delegation", fxcDelegationStarted(uuid.New(), "all", "not-a-uuid"))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Len(t, calls, 1)
}

func TestDelegationStarted_EachKnownScopeIsStillAccepted(t *testing.T) {
	for _, scope := range []string{"all", "department", "tender"} {
		t.Run(scope, func(t *testing.T) {
			fakes := newEventsFakes()
			var calls []port.DelegationRerouteInput
			fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
				calls = append(calls, in)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))

			w := postEventTo(router, "/api/v1/internal/events/delegation", fxcDelegationStarted(uuid.New(), scope, uuid.NewString()))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, calls, 1)
			assert.Equal(t, scope, calls[0].Scope)
		})
	}
}
