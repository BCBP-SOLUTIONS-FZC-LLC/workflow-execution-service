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

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type lockRecordingRecency struct {
	*fakeRecencyGuard
	scopes []string
	held   bool
}

func (l *lockRecordingRecency) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	l.scopes = append(l.scopes, scopeKey)
	l.held = true
	defer func() { l.held = false }()
	return fn(ctx)
}

func tenantStateBody(eventID, tenantID uuid.UUID, at time.Time, status string) map[string]any {
	return envelope("TenantStateChanged", eventID, tenantID, at, map[string]any{
		"tenant_id": tenantID.String(), "status": status, "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": at.Format(time.RFC3339), "cause": "x",
	})
}

func availabilityBody(at time.Time, status string) map[string]any {
	return envelope("UserAvailabilityChanged", uuid.New(), testTenantID, at, map[string]any{
		"user_id": testUserID.String(), "status": status,
	})
}

func TestHandleInternalEvent_UserAvailabilityChanged_UnknownStatus_ChangesNothingNotEvenRecency(t *testing.T) {
	for _, status := range []string{"away", "OOO", ""} {
		t.Run(fmt.Sprintf("status %q", status), func(t *testing.T) {
			fakes := newEventsFakes()
			var applied []string
			fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error {
				applied = append(applied, in.Status)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			newer := time.Now().Truncate(time.Second)
			earlier := newer.Add(-time.Hour)

			require.Equal(t, http.StatusOK, postEvent(router, availabilityBody(newer, status)).Code)
			require.Equal(t, http.StatusOK, postEvent(router, availabilityBody(earlier, "ooo")).Code)

			assert.Equal(t, []string{"ooo"}, applied, "the unrecognised status is acknowledged and ignored, and the earlier valid event still applies")
		})
	}
}

func TestHandleInternalEvent_TenantStateChanged_StaleOffboarded_IsSkipped(t *testing.T) {
	fakes := newEventsFakes()
	var statuses []string
	fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
		statuses = append(statuses, in.Status)
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))
	newer := time.Now().Truncate(time.Second)
	older := newer.Add(-24 * time.Hour)

	require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, newer, "active")).Code)
	require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, older, "offboarded")).Code)

	assert.Equal(t, []string{"active"}, statuses, "an offboarding older than the tenant's return must not terminate its instances")
}

func TestHandleInternalEvent_TenantMembershipsPurged_StaleEvent_IsSkipped(t *testing.T) {
	fakes := newEventsFakes()
	var statuses []string
	fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
		statuses = append(statuses, in.Status)
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))
	newer := time.Now().Truncate(time.Second)
	older := newer.Add(-24 * time.Hour)

	require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, newer, "active")).Code)
	require.Equal(t, http.StatusOK, postEvent(router, envelope("TenantMembershipsPurged", uuid.New(), testTenantID, older, map[string]any{})).Code)

	assert.Equal(t, []string{"active"}, statuses, "a purge signal older than the tenant's return must not terminate its instances")
}

func TestHandleInternalEvent_TenantMembershipsPurged_CommitsRecencyAfterApply(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))
	at := time.Now().Truncate(time.Second)

	require.Equal(t, http.StatusOK, postEvent(router, envelope("TenantMembershipsPurged", uuid.New(), testTenantID, at, map[string]any{})).Code)

	shouldApply, err := fakes.recency.ShouldApply(context.Background(), "tenant:"+testTenantID.String(), at)
	require.NoError(t, err)
	assert.False(t, shouldApply, "an applied purge moves the tenant's recency value")
}

func TestHandleInternalEvent_TenantEvents_RunUnderTheSameTenantLock(t *testing.T) {
	bodies := map[string]func() map[string]any{
		"TenantStateChanged": func() map[string]any {
			return tenantStateBody(uuid.New(), testTenantID, time.Now(), "suspended")
		},
		"TenantMembershipsPurged": func() map[string]any {
			return envelope("TenantMembershipsPurged", uuid.New(), testTenantID, time.Now(), map[string]any{})
		},
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			recency := &lockRecordingRecency{fakeRecencyGuard: fakes.recency}
			heldDuringApply := false
			fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
				heldDuringApply = recency.held
				return nil
			}
			router := newInternalRouter(newEventsHandlerWithRecency(fakes, recency))

			require.Equal(t, http.StatusOK, postEvent(router, body()).Code)

			assert.Equal(t, []string{"tenant:" + testTenantID.String()}, recency.scopes)
			assert.True(t, heldDuringApply, "the sweep runs while the tenant lock is held")
		})
	}
}

func TestHandleInternalEvent_TenantStateChanged_UnknownStatus_ChangesNothingNotEvenRecency(t *testing.T) {
	for _, status := range []string{"Suspended", "OFFBOARDED", "frozen", ""} {
		t.Run(fmt.Sprintf("status %q", status), func(t *testing.T) {
			fakes := newEventsFakes()
			var statuses []string
			fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
				statuses = append(statuses, in.Status)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			newer := time.Now().Truncate(time.Second)
			earlier := newer.Add(-time.Hour)

			require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, newer, status)).Code)
			require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, earlier, "suspended")).Code)

			assert.Equal(t, []string{"suspended"}, statuses, "the unrecognised status is acknowledged and ignored, and the earlier valid event still applies")
		})
	}
}

func TestHandleInternalEvent_TenantEvents_PayloadTenantDifferentFromEnvelope_400(t *testing.T) {
	otherTenant := uuid.New()
	cases := map[string]map[string]any{
		"TenantStateChanged": envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
			"tenant_id": otherTenant.String(), "status": "offboarded", "previous_status": "active",
			"plan": "pro", "previous_plan": "pro", "changed_at": time.Now().Format(time.RFC3339), "cause": "x",
		}),
		"TenantMembershipsPurged": envelope("TenantMembershipsPurged", uuid.New(), testTenantID, time.Now(), map[string]any{
			"tenant_id": otherTenant.String(),
		}),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			called := false
			fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
				called = true
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))

			w := postEvent(router, body)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.False(t, called, "a payload naming another tenant reconciles nothing")
		})
	}
}

func TestHandleInternalEvent_TenantMembershipsPurged_PayloadTenantEqualToEnvelope_Applies(t *testing.T) {
	fakes := newEventsFakes()
	called := false
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
		called = true
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("TenantMembershipsPurged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": testTenantID.String(),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called)
}

func TestHandleInternalEvent_TenantStateChanged_ZeroChangedAt_400(t *testing.T) {
	fakes := newEventsFakes()
	called := false
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
		called = true
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": "0001-01-01T00:00:00Z", "cause": "x",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, called)
}

func TestHandleInternalEvent_ReconcilerFailure_UpstreamAnswers503AndIsCounted(t *testing.T) {
	const metric = "workflow_execution_service_upstream_dependency_errors_total"
	upstreamErr := fmt.Errorf("%w: token service answered 502", adapterhttp.ErrUpstreamUnavailable)
	cases := map[string]func(*eventsFakes){
		"TenantStateChanged": func(f *eventsFakes) {
			f.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error { return upstreamErr }
		},
		"UserDeleted": func(f *eventsFakes) {
			f.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error { return upstreamErr }
		},
		"UserAvailabilityChanged": func(f *eventsFakes) {
			f.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error { return upstreamErr }
		},
	}
	bodies := map[string]map[string]any{
		"TenantStateChanged": tenantStateBody(uuid.New(), testTenantID, time.Now(), "suspended"),
		"UserDeleted": envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
			"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
		}),
		"UserAvailabilityChanged": availabilityBody(time.Now(), "ooo"),
	}
	for name, fail := range cases {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			fail(fakes)
			router := newInternalRouter(newEventsHandler(fakes))
			before := obstest.Sum(t, metric, map[string]string{"dependency": "iam"})

			w := postEvent(router, bodies[name])

			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			assert.Contains(t, w.Body.String(), "UPSTREAM_UNAVAILABLE")
			assert.Equal(t, before+1, obstest.Sum(t, metric, map[string]string{"dependency": "iam"}))
		})
	}
}

func TestHandleInternalEvent_ReconcilerFailure_OtherErrorsStay500AndAreNotCounted(t *testing.T) {
	const metric = "workflow_execution_service_upstream_dependency_errors_total"
	fakes := newEventsFakes()
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error { return errors.New("boom") }
	router := newInternalRouter(newEventsHandler(fakes))
	before := obstest.Sum(t, metric, nil)

	w := postEvent(router, tenantStateBody(uuid.New(), testTenantID, time.Now(), "suspended"))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, before, obstest.Sum(t, metric, nil))
}

func TestHandleInternalEvent_Offboarded_AppliesOnATieAndSkipsOnlyWhenStrictlyOlder(t *testing.T) {
	for _, source := range []string{"TenantStateChanged", "TenantMembershipsPurged"} {
		t.Run(source, func(t *testing.T) {
			fakes := newEventsFakes()
			var statuses []string
			fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
				statuses = append(statuses, in.Status)
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			at := time.Now().Truncate(time.Second)
			offboard := func(when time.Time) map[string]any {
				if source == "TenantMembershipsPurged" {
					return envelope(source, uuid.New(), testTenantID, when, map[string]any{})
				}
				return tenantStateBody(uuid.New(), testTenantID, when, "offboarded")
			}

			require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, at, "cancelled")).Code)
			require.Equal(t, http.StatusOK, postEvent(router, offboard(at)).Code)
			require.Equal(t, http.StatusOK, postEvent(router, offboard(at.Add(-time.Second))).Code)

			assert.Equal(t, []string{"cancelled", "offboarded"}, statuses, "an offboarding at the time of the last change applies; a strictly older one does not")
		})
	}
}

func TestHandleInternalEvent_NonTerminalStatusOnATie_StaysSkipped(t *testing.T) {
	fakes := newEventsFakes()
	var statuses []string
	fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
		statuses = append(statuses, in.Status)
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))
	at := time.Now().Truncate(time.Second)

	require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, at, "cancelled")).Code)
	require.Equal(t, http.StatusOK, postEvent(router, tenantStateBody(uuid.New(), testTenantID, at, "active")).Code)

	assert.Equal(t, []string{"cancelled"}, statuses)
}

func TestHandleInternalEvent_TenantMembershipsPurged_UndecodablePayload_400(t *testing.T) {
	cases := map[string]any{
		"tenant_id of the wrong type":   map[string]any{"tenant_id": 5},
		"payload that is not an object": "not-an-object",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			called := false
			fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
				called = true
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			body := envelope("TenantMembershipsPurged", uuid.New(), testTenantID, time.Now(), nil)
			body["data"] = data

			w := postEvent(router, body)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.False(t, called)
		})
	}
}

func TestHandleInternalEvent_TenantMembershipsPurged_EmptyPayloads_StillApply(t *testing.T) {
	for name, data := range map[string]any{"empty object": map[string]any{}, "null": nil} {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			called := false
			fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
				called = true
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			body := envelope("TenantMembershipsPurged", uuid.New(), testTenantID, time.Now(), nil)
			body["data"] = data

			require.Equal(t, http.StatusOK, postEvent(router, body).Code)
			assert.True(t, called)
		})
	}
}

func TestHandleInternalEvent_UnrecognisedStatus_IsCountedAsIgnoredNotOk(t *testing.T) {
	const metric = "workflow_execution_service_internal_events_ingest_total"
	obstest.Init(t)
	cases := map[string]map[string]any{
		"TenantStateChanged":      tenantStateBody(uuid.New(), testTenantID, time.Now(), "Suspended"),
		"UserAvailabilityChanged": availabilityBody(time.Now(), "away"),
	}
	for eventType, body := range cases {
		t.Run(eventType, func(t *testing.T) {
			router := newInternalRouter(newEventsHandler(newEventsFakes()))
			ignored := map[string]string{"event_type": eventType, "result": "ignored"}
			ok := map[string]string{"event_type": eventType, "result": "ok"}
			ignoredBefore, okBefore := obstest.Sum(t, metric, ignored), obstest.Sum(t, metric, ok)

			require.Equal(t, http.StatusOK, postEvent(router, body).Code)

			assert.Equal(t, ignoredBefore+1, obstest.Sum(t, metric, ignored))
			assert.Equal(t, okBefore, obstest.Sum(t, metric, ok))
		})
	}
}
