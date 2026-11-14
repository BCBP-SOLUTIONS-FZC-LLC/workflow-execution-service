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

// hhaEventBody builds an envelope with an extra top-level member that this
// service does not read.
func hhaEventBody(eventType string, eventID, tenantID uuid.UUID, at time.Time, payload map[string]any) map[string]any {
	return map[string]any{
		"id":                  eventID.String(),
		"type":                eventType,
		"tenant_id":           tenantID.String(),
		"time":                at.Format(time.RFC3339),
		"data":                payload,
		"added_by_the_sender": map[string]any{"nested": true},
	}
}

func TestDelegationEvents_ExtraEnvelopeAndPayloadFields_AreIgnored(t *testing.T) {
	fakes := newEventsFakes()
	var got []port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
		got = append(got, in)
		return nil
	}
	router := hhaInternalRouter(newEventsHandler(fakes))
	delegationID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
	tenantID := uuid.New()
	startsAt := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	eventID := uuid.New()

	body := hhaEventBody("DelegationStarted", eventID, tenantID, time.Now(), map[string]any{
		"delegation_id":       delegationID.String(),
		"delegator_id":        delegatorID.String(),
		"delegate_id":         delegateID.String(),
		"scope":               "all",
		"starts_at":           startsAt.Format(time.RFC3339),
		"added_by_the_sender": 42,
	})
	w := hhaPostJSON(router, "/api/v1/internal/events/delegation", hhaJSONString(t, body))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, got, 1, "the event must be handled once")
	assert.Equal(t, tenantID, got[0].TenantID)
	assert.Equal(t, delegationID, got[0].DelegationID)
	assert.Equal(t, delegatorID, got[0].DelegatorID)
	assert.Equal(t, delegateID, got[0].DelegateID)
	assert.Equal(t, "all", got[0].Scope)
	assert.True(t, startsAt.Equal(got[0].StartsAt), "starts_at: got %v", got[0].StartsAt)
	assert.True(t, fakes.processedEvents.seen[processedKey(eventID, port.ConsumerMembership)], "a handled event is recorded as processed")
}

func TestUserProfileEvents_ExtraEnvelopeAndPayloadFields_AreIgnored(t *testing.T) {
	tenantID, userID := uuid.New(), uuid.New()
	deletedAt := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)

	t.Run("UserDeleted", func(t *testing.T) {
		fakes := newEventsFakes()
		var got []port.UserDeletedInput
		fakes.userSafetyNet.vacateAssignments = func(_ context.Context, in port.UserDeletedInput) error {
			got = append(got, in)
			return nil
		}
		router := hhaInternalRouter(newEventsHandler(fakes))
		eventID := uuid.New()

		body := hhaEventBody("UserDeleted", eventID, tenantID, time.Now(), map[string]any{
			"user_id":             userID.String(),
			"deleted_at":          deletedAt.Format(time.RFC3339),
			"added_by_the_sender": []string{"x"},
		})
		w := hhaPostJSON(router, "/api/v1/internal/events/user-profile", hhaJSONString(t, body))

		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Len(t, got, 1)
		assert.Equal(t, tenantID, got[0].TenantID)
		assert.Equal(t, userID, got[0].UserID)
		assert.True(t, deletedAt.Equal(got[0].DeletedAt), "deleted_at: got %v", got[0].DeletedAt)
		assert.True(t, fakes.processedEvents.seen[processedKey(eventID, port.ConsumerUser)])
	})

	t.Run("UserAvailabilityChanged", func(t *testing.T) {
		fakes := newEventsFakes()
		var got []port.UserAvailabilityInput
		fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error {
			got = append(got, in)
			return nil
		}
		router := hhaInternalRouter(newEventsHandler(fakes))
		eventID := uuid.New()
		at := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)

		body := hhaEventBody("UserAvailabilityChanged", eventID, tenantID, at, map[string]any{
			"user_id":             userID.String(),
			"status":              "ooo",
			"added_by_the_sender": map[string]any{"a": 1},
		})
		w := hhaPostJSON(router, "/api/v1/internal/events/user-profile", hhaJSONString(t, body))

		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Len(t, got, 1)
		assert.Equal(t, tenantID, got[0].TenantID)
		assert.Equal(t, userID, got[0].UserID)
		assert.Equal(t, "ooo", got[0].Status)
		assert.True(t, at.Equal(got[0].ChangedAt), "changed_at: got %v", got[0].ChangedAt)
		assert.True(t, fakes.processedEvents.seen[processedKey(eventID, port.ConsumerUser)])
	})
}
