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

func TestDelegationStartedEvent_CarriesTheEnvelopeTimestampAsTheTimeItWasEmitted(t *testing.T) {
	fakes := newEventsFakes()
	var got []port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
		got = append(got, in)
		return nil
	}
	router := hhaInternalRouter(newEventsHandler(fakes))
	emittedAt := time.Date(2026, 11, 17, 10, 0, 1, 0, time.UTC)
	startsAt := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

	body := hhaEventBody("DelegationStarted", uuid.New(), uuid.New(), emittedAt, map[string]any{
		"delegation_id": uuid.NewString(), "delegator_id": uuid.NewString(), "delegate_id": uuid.NewString(),
		"scope": "all", "starts_at": startsAt.Format(time.RFC3339),
	})
	w := hhaPostJSON(router, "/api/v1/internal/events/delegation", hhaJSONString(t, body))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, got, 1)
	assert.True(t, emittedAt.Equal(got[0].EmittedAt), "emitted at: got %v", got[0].EmittedAt)
	assert.True(t, startsAt.Equal(got[0].StartsAt), "the delegation's own start date is kept apart: got %v", got[0].StartsAt)
}
