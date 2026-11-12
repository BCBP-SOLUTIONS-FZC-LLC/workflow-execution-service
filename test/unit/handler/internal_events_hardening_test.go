package handler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

// The row-level-security setting is built from the parsed UUID, never from the
// raw envelope string: uuid.Parse also accepts the urn: form, braces, upper case
// and no dashes, and none of those should reach the database setting.
func TestInternalEvents_RLSSettingUsesTheCanonicalTenantID(t *testing.T) {
	odd := "urn:uuid:" + testTenantID.String()
	want := testTenantID.String()
	now := time.Now()

	cases := map[string]struct {
		body  func() map[string]any
		wire  func(f *eventsFakes, got *string)
		event string
	}{
		"DelegationStarted": {
			body: func() map[string]any {
				return envelope("DelegationStarted", uuid.New(), testTenantID, now, map[string]any{
					"delegation_id": testDelegationID2, "delegator_id": testDelegatorID, "delegate_id": testDelegateID2,
					"scope": "department", "scope_id": testDelegatorID.String(), "starts_at": now.Format(time.RFC3339),
				})
			},
			wire: func(f *eventsFakes, got *string) {
				f.delegation.reroute = func(ctx context.Context, _ port.DelegationRerouteInput) error { *got = gucTenant(ctx); return nil }
			},
		},
		"DelegationEnded": {
			body: func() map[string]any {
				return envelope("DelegationEnded", uuid.New(), testTenantID, now, map[string]any{
					"delegation_id": testDelegationID2, "delegator_id": testDelegatorID, "delegate_id": testDelegateID2, "ended_reason": "expired",
				})
			},
			wire: func(f *eventsFakes, got *string) {
				f.delegation.reverse = func(ctx context.Context, _ port.DelegationReversalInput) error { *got = gucTenant(ctx); return nil }
			},
		},
		"UserDeleted": {
			body: func() map[string]any {
				return envelope("UserDeleted", uuid.New(), testTenantID, now, map[string]any{
					"user_id": uuid.New().String(), "deleted_at": now.Format(time.RFC3339),
				})
			},
			wire: func(f *eventsFakes, got *string) {
				f.userSafetyNet.vacateAssignments = func(ctx context.Context, _ port.UserDeletedInput) error { *got = gucTenant(ctx); return nil }
			},
		},
		"MembershipRevoked": {
			body: func() map[string]any {
				return envelope("MembershipRevoked", uuid.New(), testTenantID, now, map[string]any{"user_id": testUserID.String()})
			},
			wire: func(f *eventsFakes, got *string) {
				f.userSafetyNet.vacateAssignments = func(ctx context.Context, _ port.UserDeletedInput) error { *got = gucTenant(ctx); return nil }
			},
		},
		"TenantMembershipsPurged": {
			body: func() map[string]any {
				return envelope("TenantMembershipsPurged", uuid.New(), testTenantID, now, map[string]any{})
			},
			wire: func(f *eventsFakes, got *string) {
				f.tenantLifecycle.apply = func(ctx context.Context, _ port.TenantLifecycleInput) error { *got = gucTenant(ctx); return nil }
			},
		},
		"TenantStateChanged": {
			body: func() map[string]any {
				return envelope("TenantStateChanged", uuid.New(), testTenantID, now, map[string]any{
					"tenant_id": testTenantID.String(), "status": "active", "changed_at": now.Format(time.RFC3339),
				})
			},
			wire: func(f *eventsFakes, got *string) {
				f.tenantLifecycle.apply = func(ctx context.Context, _ port.TenantLifecycleInput) error { *got = gucTenant(ctx); return nil }
			},
		},
		"UserAvailabilityChanged": {
			body: func() map[string]any {
				return envelope("UserAvailabilityChanged", uuid.New(), testTenantID, now, map[string]any{"user_id": testUserID.String(), "status": "ooo"})
			},
			wire: func(f *eventsFakes, got *string) {
				f.oooAvailability.apply = func(ctx context.Context, _ port.UserAvailabilityInput) error { *got = gucTenant(ctx); return nil }
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			var got string
			tc.wire(fakes, &got)
			body := tc.body()
			body["tenant_id"] = odd

			w := postEvent(newInternalRouter(newEventsHandler(fakes)), body)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Equal(t, want, got)
		})
	}
}

func gucTenant(ctx context.Context) string {
	g, _ := pgcommon.GUCSetFromContext(ctx)
	return g.TenantID
}

// An event type the service does not handle is request-controlled text; as a
// metric label it would let a caller create a series per value.
func TestInternalEvents_UnhandledTypeUsesTheUnknownLabel(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_internal_events_ingest_total"
	rawType := "Made" + uuid.NewString()
	before := obstest.Sum(t, metric, map[string]string{"event_type": "unknown", "result": "unhandled_type"})
	router := newInternalRouter(newEventsHandler(newEventsFakes()))

	w := postEventTo(router, "/api/v1/internal/events/tenant", envelope(rawType, uuid.New(), testTenantID, time.Now(), map[string]any{}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, before+1, obstest.Sum(t, metric, map[string]string{"event_type": "unknown", "result": "unhandled_type"}))
	assert.Zero(t, obstest.Sum(t, metric, map[string]string{"event_type": rawType}), "the raw type is not a label value")
	assert.Zero(t, obstest.Sum(t, "workflow_execution_service_internal_events_last_received_timestamp", map[string]string{"event_type": rawType}))
}

// The event type is request-controlled text until the type check has run: the
// refusals that happen before it (no timestamp, a payload that cannot be decoded)
// must not use it as a label value either.
func TestInternalEvents_RefusalsBeforeTheTypeCheckUseTheUnknownLabel(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_internal_events_ingest_total"
	cases := map[string]struct {
		status int
		result string
		body   func(rawType string) map[string]any
	}{
		"no timestamp": {
			status: http.StatusBadRequest, result: "bad_payload",
			body: func(rawType string) map[string]any {
				b := envelope(rawType, uuid.New(), testTenantID, time.Now(), map[string]any{})
				delete(b, "time")
				return b
			},
		},
		"schema payload that is not base64 text": {
			status: http.StatusBadGateway, result: "decode_failed",
			body: func(rawType string) map[string]any {
				return envelopeWithSchemaID(rawType, uuid.New(), testTenantID, time.Now(), "schema-1", 42)
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rawType := "Made" + uuid.NewString()
			unknown := map[string]string{"event_type": "unknown", "result": tc.result}
			before := obstest.Sum(t, metric, unknown)
			router := newInternalRouter(newEventsHandler(newEventsFakes()))

			w := postEventTo(router, "/api/v1/internal/events/tenant", tc.body(rawType))

			require.Equal(t, tc.status, w.Code, w.Body.String())
			assert.Equal(t, before+1, obstest.Sum(t, metric, unknown))
			assert.Zero(t, obstest.Sum(t, metric, map[string]string{"event_type": rawType}), "the raw type is not a label value")
		})
	}
}

// The Go dial text names a host, a port and a path; none of it belongs in a body.
func TestMapErr_UpstreamUnavailableDetailHidesItsCause(t *testing.T) {
	cause := fmt.Errorf("%w: token service: %w", adapterhttp.ErrUpstreamUnavailable,
		errors.New(`Post "http://token-service.iam.svc:8080/internal/principals": dial tcp 10.2.3.4:8080: connect: connection refused`))
	pauser := &recordingUserTaskPauser{err: cause}

	w := doPause(t, pauser)

	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), handlerCodeUpstream)
	for _, leaked := range []string{"token-service", "10.2.3.4", "8080", "dial tcp", "connection refused"} {
		assert.NotContains(t, w.Body.String(), leaked)
	}
}
