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

func TestHandleInternalEvent_TenantMembershipsPurged_InvalidEnvelopeIDs_Returns400(t *testing.T) {
	cases := []struct {
		name       string
		field      string
		wantDetail string
	}{
		{"event id", "id", "invalid event id"},
		{"tenant id", "tenant_id", "invalid tenant_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakes := newEventsFakes()
			applied := false
			fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
				applied = true
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			eventID := uuid.New()
			body := envelope("TenantMembershipsPurged", eventID, testTenantID, time.Now(), map[string]any{})
			body[tc.field] = "not-a-uuid"

			w := postEvent(router, body)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.False(t, applied, "an event with a bad envelope id must not drive the offboard cascade")
			assert.Empty(t, fakes.processedEvents.seen, "a refused event must not be recorded as processed")
		})
	}
}
