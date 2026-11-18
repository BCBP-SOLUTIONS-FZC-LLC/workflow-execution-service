package handler_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var errW17aInboxDown = errors.New("w17a: inbox store is down")

// w17aDelegationEventCase is one of the two events the delegation route takes, with the
// reconciler call the handler is expected to make for it when the envelope is stamped at.
type w17aDelegationEventCase struct {
	name      string
	eventType string
	data      func(delegationID, delegatorID, delegateID uuid.UUID) map[string]any
	want      func(tenantID, delegationID, delegatorID, delegateID uuid.UUID, at time.Time) w17aDelegationCall
}

func w17aDelegationEventCases() []w17aDelegationEventCase {
	return []w17aDelegationEventCase{
		{
			name: "DelegationStarted", eventType: "DelegationStarted", data: w17aStartedData,
			want: func(tenantID, delegationID, delegatorID, delegateID uuid.UUID, at time.Time) w17aDelegationCall {
				return w17aDelegationCall{
					rerouted: true, tenantID: tenantID, delegationID: delegationID, delegatorID: delegatorID,
					delegateID: delegateID, scope: "department", scopeID: w17aScopeID,
					startsAt: "2030-01-02T03:04:05Z", endsAt: "2030-02-03T04:05:06Z", emittedAt: w17aTime(at),
					gucTenant: tenantID.String(),
				}
			},
		},
		{
			name: "DelegationEnded", eventType: "DelegationEnded", data: w17aEndedData,
			want: func(tenantID, delegationID, delegatorID, delegateID uuid.UUID, _ time.Time) w17aDelegationCall {
				return w17aDelegationCall{
					tenantID: tenantID, delegationID: delegationID, delegatorID: delegatorID,
					delegateID: delegateID, endedReason: "expired", gucTenant: tenantID.String(),
				}
			},
		},
	}
}

func TestDelegationEvent_UpstreamUnavailable_Answers503AndTheRedeliveryIsHandled(t *testing.T) {
	obstest.Init(t)
	const upstreamMetric = "workflow_execution_service_upstream_dependency_errors_total"
	cases := []struct {
		name       string
		err        error
		wantDetail string
		leaked     []string
	}{
		{
			name: "the token service cannot be reached",
			err: fmt.Errorf("%w: token service: dial tcp 10.2.3.4:8080: connect: connection refused",
				adapterhttp.ErrUpstreamUnavailable),
			wantDetail: "an upstream service could not serve the request",
			leaked:     []string{"token service", "10.2.3.4", "dial tcp", "connection refused"},
		},
		{
			name:       "the eligibility service answers a different number of results",
			err:        fmt.Errorf("%w: delegation reroute: 1 answers for 2 requests", port.ErrEligibilityAnswerCount),
			wantDetail: "the eligibility check did not answer every assignee; retry the call",
			leaked:     []string{"answers for", "delegation reroute"},
		},
	}
	for _, ev := range w17aDelegationEventCases() {
		for _, tc := range cases {
			t.Run(ev.name+": "+tc.name, func(t *testing.T) {
				rig := w17aNewRig()
				rig.delegation.results = []error{tc.err}
				tenantID, delegationID, delegatorID, delegateID, eventID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
				at := time.Now().UTC().Truncate(time.Second)
				env := envelope(ev.eventType, eventID, tenantID, at, ev.data(delegationID, delegatorID, delegateID))
				wantCall := ev.want(tenantID, delegationID, delegatorID, delegateID, at)
				failed := map[string]string{"event_type": ev.eventType, "result": "error"}
				succeeded := map[string]string{"event_type": ev.eventType, "result": "ok"}
				failedBefore, okBefore := obstest.Sum(t, w17aIngestMetric, failed), obstest.Sum(t, w17aIngestMetric, succeeded)
				upstreamBefore := obstest.Sum(t, upstreamMetric, map[string]string{"dependency": "iam"})

				w := rig.postEnvelope(w17aDelegationRoute, env)

				require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
				p := w17aDecodeProblem(t, w)
				assert.Equal(t, http.StatusServiceUnavailable, p.Status)
				assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
				assert.Equal(t, "https://errors.bcbp.io/execution/upstream-unavailable", p.Type)
				assert.Equal(t, "Service Unavailable", p.Title)
				assert.Equal(t, tc.wantDetail, p.Detail)
				for _, leaked := range tc.leaked {
					assert.NotContains(t, w.Body.String(), leaked, "the error text is not echoed")
				}
				assert.Equal(t, []w17aDelegationCall{wantCall}, rig.delegation.calls)
				assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.lookups)
				assert.Empty(t, rig.inbox.marks, "the event is not marked processed, so it is delivered again")
				assert.Equal(t, failedBefore+1, obstest.Sum(t, w17aIngestMetric, failed))
				assert.Equal(t, okBefore, obstest.Sum(t, w17aIngestMetric, succeeded))
				assert.Equal(t, upstreamBefore+1, obstest.Sum(t, upstreamMetric, map[string]string{"dependency": "iam"}))

				w = rig.postEnvelope(w17aDelegationRoute, env)

				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				assert.Equal(t, []w17aDelegationCall{wantCall, wantCall}, rig.delegation.calls, "the redelivery reaches the reconciler for the same tenant and delegation")
				assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.marks)
				assert.Equal(t, okBefore+1, obstest.Sum(t, w17aIngestMetric, succeeded))
			})
		}
	}
}

func TestDelegationEvent_InboxLookupFails_ProceedsAndLogsAWarning(t *testing.T) {
	obstest.Init(t)
	for _, ev := range w17aDelegationEventCases() {
		t.Run(ev.name, func(t *testing.T) {
			rig := w17aNewRig()
			rig.inbox.lookupErr = errW17aInboxDown
			tenantID, delegationID, delegatorID, delegateID, eventID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			at := time.Now().UTC().Truncate(time.Second)
			succeeded := map[string]string{"event_type": ev.eventType, "result": "ok"}
			okBefore := obstest.Sum(t, w17aIngestMetric, succeeded)

			w := rig.postEnvelope(w17aDelegationRoute, envelope(ev.eventType, eventID, tenantID, at, ev.data(delegationID, delegatorID, delegateID)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Empty(t, w.Body.String())
			assert.Equal(t, []w17aDelegationCall{ev.want(tenantID, delegationID, delegatorID, delegateID, at)}, rig.delegation.calls,
				"a lookup that cannot be answered does not block the event")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.lookups)
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.marks)
			warnings := rig.log.at("warn")
			require.Len(t, warnings, 1)
			assert.Equal(t, "internal events: IsProcessed check failed, proceeding", warnings[0].msg)
			assert.Equal(t, ev.eventType, warnings[0].fields["event_type"])
			assert.Equal(t, eventID, warnings[0].fields["event_id"])
			assert.Equal(t, errW17aInboxDown.Error(), warnings[0].fields["error"])
			assert.Equal(t, okBefore+1, obstest.Sum(t, w17aIngestMetric, succeeded))
		})
	}
}

func TestDelegationEvent_MarkProcessedFails_StillAnswers200AndLogsAWarning(t *testing.T) {
	obstest.Init(t)
	for _, ev := range w17aDelegationEventCases() {
		t.Run(ev.name, func(t *testing.T) {
			rig := w17aNewRig()
			rig.inbox.markErr = errW17aInboxDown
			tenantID, delegationID, delegatorID, delegateID, eventID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			at := time.Now().UTC().Truncate(time.Second)
			succeeded := map[string]string{"event_type": ev.eventType, "result": "ok"}
			failed := map[string]string{"event_type": ev.eventType, "result": "error"}
			okBefore, failedBefore := obstest.Sum(t, w17aIngestMetric, succeeded), obstest.Sum(t, w17aIngestMetric, failed)

			w := rig.postEnvelope(w17aDelegationRoute, envelope(ev.eventType, eventID, tenantID, at, ev.data(delegationID, delegatorID, delegateID)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Empty(t, w.Body.String(), "the committed work is acknowledged without a body")
			assert.Equal(t, []w17aDelegationCall{ev.want(tenantID, delegationID, delegatorID, delegateID, at)}, rig.delegation.calls)
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.marks, "the mark was attempted for this event")
			warnings := rig.log.at("warn")
			require.Len(t, warnings, 1)
			assert.Equal(t, "internal events: MarkProcessed failed after side effects committed", warnings[0].msg)
			assert.Equal(t, ev.eventType, warnings[0].fields["event_type"])
			assert.Equal(t, eventID, warnings[0].fields["event_id"])
			assert.Equal(t, errW17aInboxDown.Error(), warnings[0].fields["error"])
			assert.Equal(t, okBefore+1, obstest.Sum(t, w17aIngestMetric, succeeded))
			assert.Equal(t, failedBefore, obstest.Sum(t, w17aIngestMetric, failed))
		})
	}
}

func TestDelegationEvent_NonJSONContentType_Answers415AndDoesNothing(t *testing.T) {
	obstest.Init(t)
	unknownBadPayload := map[string]string{"event_type": "unknown", "result": "bad_payload"}
	for _, ev := range w17aDelegationEventCases() {
		for _, contentType := range []string{"text/plain", "text/plain; charset=utf-8", "application/xml"} {
			t.Run(ev.name+" as "+contentType, func(t *testing.T) {
				rig := w17aNewRig()
				env := envelope(ev.eventType, uuid.New(), uuid.New(), time.Now(), ev.data(uuid.New(), uuid.New(), uuid.New()))
				before := obstest.Sum(t, w17aIngestMetric, unknownBadPayload)

				w := rig.postWithType(w17aDelegationRoute, contentType, w17aMarshal(t, env))

				require.Equal(t, http.StatusUnsupportedMediaType, w.Code, w.Body.String())
				p := w17aDecodeProblem(t, w)
				assert.Equal(t, http.StatusUnsupportedMediaType, p.Status)
				assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", p.Code)
				assert.Equal(t, "https://errors.bcbp.io/execution/unsupported-media-type", p.Type)
				assert.Equal(t, "Unsupported Media Type", p.Title)
				assert.Equal(t, "request body must be application/json", p.Detail)
				assert.True(t, rig.untouched(), "no reconciler or inbox write ran")
				assert.Empty(t, rig.inbox.lookups)
				assert.Equal(t, before+1, obstest.Sum(t, w17aIngestMetric, unknownBadPayload))
			})
		}
	}
}

func TestDelegationEvent_ReconcilerGetsTheEventTenantInItsInputAndItsTenantSetting(t *testing.T) {
	for _, ev := range w17aDelegationEventCases() {
		t.Run(ev.name, func(t *testing.T) {
			rig := w17aNewRig()
			tenantID := uuid.New()
			delegationID, delegatorID, delegateID := uuid.New(), uuid.New(), uuid.New()
			at := time.Now().UTC().Truncate(time.Second)

			w := rig.postEnvelope(w17aDelegationRoute, envelope(ev.eventType, uuid.New(), tenantID, at, ev.data(delegationID, delegatorID, delegateID)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Equal(t, []w17aDelegationCall{ev.want(tenantID, delegationID, delegatorID, delegateID, at)}, rig.delegation.calls)
			assert.Equal(t, tenantID.String(), rig.delegation.calls[0].gucTenant, "the tenant setting on the context is the event's tenant")
		})
	}
}
