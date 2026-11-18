package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w12aProblem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
	Code   string `json:"code"`
}

func w12aDecodeProblem(t *testing.T, w *httptest.ResponseRecorder) w12aProblem {
	t.Helper()
	var p w12aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	return p
}

// w12aDelegationEvent is one of the two events the delegation route takes, with the
// reconciler call it reaches replaced by a function that fails with err, and a record
// of the tenant and delegation it was called with.
type w12aDelegationEvent struct {
	name string
	body func(eventID uuid.UUID) map[string]any
	fail func(f *eventsFakes, err error, calls *[]w12aDelegationCall)
}

type w12aDelegationCall struct {
	tenantID     uuid.UUID
	delegationID uuid.UUID
}

func w12aDelegationEvents() []w12aDelegationEvent {
	return []w12aDelegationEvent{
		{
			name: "DelegationStarted",
			body: func(eventID uuid.UUID) map[string]any {
				return envelope("DelegationStarted", eventID, testTenantID, time.Now(), map[string]any{
					"delegation_id": testDelegationID2, "delegator_id": testDelegatorID, "delegate_id": testDelegateID2,
					"scope": "all", "starts_at": time.Now().Format(time.RFC3339),
				})
			},
			fail: func(f *eventsFakes, err error, calls *[]w12aDelegationCall) {
				f.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
					*calls = append(*calls, w12aDelegationCall{tenantID: in.TenantID, delegationID: in.DelegationID})
					return err
				}
			},
		},
		{
			name: "DelegationEnded",
			body: func(eventID uuid.UUID) map[string]any {
				return envelope("DelegationEnded", eventID, testTenantID, time.Now(), map[string]any{
					"delegation_id": testDelegationID2, "delegator_id": testDelegatorID, "delegate_id": testDelegateID2,
					"ended_reason": "expired",
				})
			},
			fail: func(f *eventsFakes, err error, calls *[]w12aDelegationCall) {
				f.delegation.reverse = func(_ context.Context, in port.DelegationReversalInput) error {
					*calls = append(*calls, w12aDelegationCall{tenantID: in.TenantID, delegationID: in.DelegationID})
					return err
				}
			},
		},
	}
}

func TestDelegationEvent_ReconcilerFailure_IsAProblemBodyWithoutTheErrorTextAndTheEventIsRedelivered(t *testing.T) {
	const rawCause = "pq: could not serialize access: tenant 4f2 row 77"
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantType   string
		wantTitle  string
		wantCode   string
		wantDetail string
	}{
		{
			name:       "the vacate or the read of the task's assignments fails",
			err:        fmt.Errorf("delegation reconciler: %w", errors.New(rawCause)),
			wantStatus: http.StatusInternalServerError,
			wantType:   "https://errors.bcbp.io/execution/internal-error",
			wantTitle:  "Internal Server Error",
			wantCode:   "INTERNAL_ERROR",
			wantDetail: "failed to process event",
		},
		{
			name:       "a stale lead move makes the event redeliver instead of answering 409",
			err:        fmt.Errorf("delegation reconciler: %w", domain.ErrRecordVersionConflict),
			wantStatus: http.StatusInternalServerError,
			wantType:   "https://errors.bcbp.io/execution/internal-error",
			wantTitle:  "Internal Server Error",
			wantCode:   "INTERNAL_ERROR",
			wantDetail: "failed to process event",
		},
		{
			name:       "the eligibility service answers a different number of results",
			err:        fmt.Errorf("%w: delegation reroute: 1 answers for 2 requests", port.ErrEligibilityAnswerCount),
			wantStatus: http.StatusServiceUnavailable,
			wantType:   "https://errors.bcbp.io/execution/upstream-unavailable",
			wantTitle:  "Service Unavailable",
			wantCode:   "UPSTREAM_UNAVAILABLE",
			wantDetail: "the eligibility check did not answer every assignee; retry the call",
		},
	}
	for _, ev := range w12aDelegationEvents() {
		for _, tc := range cases {
			t.Run(ev.name+": "+tc.name, func(t *testing.T) {
				fakes := newEventsFakes()
				var calls []w12aDelegationCall
				ev.fail(fakes, tc.err, &calls)
				router := newInternalRouter(newEventsHandler(fakes))
				eventID := uuid.New()

				w := postEventTo(router, "/api/v1/internal/events/delegation", ev.body(eventID))

				require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
				assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
				p := w12aDecodeProblem(t, w)
				assert.Equal(t, tc.wantStatus, p.Status)
				assert.Equal(t, tc.wantType, p.Type)
				assert.Equal(t, tc.wantTitle, p.Title)
				assert.Equal(t, tc.wantCode, p.Code)
				assert.Equal(t, tc.wantDetail, p.Detail)
				assert.NotContains(t, w.Body.String(), "serialize", "the error text is not echoed")
				assert.NotContains(t, w.Body.String(), "answers for", "the error text is not echoed")
				assert.NotContains(t, w.Body.String(), "record version", "the error text is not echoed")
				assert.Equal(t, []w12aDelegationCall{{tenantID: testTenantID, delegationID: testDelegationID2}}, calls, "the reconciler is called once, for the event's tenant and delegation")
				processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
				require.NoError(t, err)
				assert.False(t, processed, "the event is not marked processed, so it is delivered again")
			})
		}

		t.Run(ev.name+": a success is acknowledged and marked processed", func(t *testing.T) {
			fakes := newEventsFakes()
			var calls []w12aDelegationCall
			ev.fail(fakes, nil, &calls)
			router := newInternalRouter(newEventsHandler(fakes))
			eventID := uuid.New()

			w := postEventTo(router, "/api/v1/internal/events/delegation", ev.body(eventID))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, calls, 1)
			processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
			require.NoError(t, err)
			assert.True(t, processed)
		})
	}
}
