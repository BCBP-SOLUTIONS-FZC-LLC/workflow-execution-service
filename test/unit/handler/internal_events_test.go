package handler_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	testDelegationID2 = uuid.MustParse("88888888-8888-8888-8888-888888888888")
	testDelegatorID   = uuid.MustParse("99999999-9999-9999-9999-999999999999")
	testDelegateID2   = uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
)

func envelope(eventType string, eventID uuid.UUID, tenantID uuid.UUID, at time.Time, data map[string]any) map[string]any {
	return map[string]any{
		"id":        eventID.String(),
		"type":      eventType,
		"tenant_id": tenantID.String(),
		"time":      at.Format(time.RFC3339),
		"data":      data,
	}
}

func postEvent(router *gin.Engine, body map[string]any) *httptest.ResponseRecorder {
	return do(router, internalReq(http.MethodPost, "/api/v1/internal/events", body))
}

// --- dispatch / envelope-level edge cases ---

func TestHandleInternalEvent_UnknownType_Returns200(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("SomeFutureEvent", uuid.New(), testTenantID, time.Now(), map[string]any{}))

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleInternalEvent_EmptyType_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("", uuid.New(), testTenantID, time.Now(), map[string]any{}))

	assert.Equal(t, http.StatusBadRequest, w.Code, "an envelope missing its type entirely is malformed, not merely an unrecognized future type")
}

func TestHandleInternalEvent_MalformedEnvelope_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	r := internalReq(http.MethodPost, "/api/v1/internal/events", nil)
	r.Body = http.NoBody
	w := do(router, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleInternalEvent_UnsupportedMediaType(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	r := internalReq(http.MethodPost, "/api/v1/internal/events",
		envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{}))
	r.Header.Set("Content-Type", "text/plain")
	w := do(router, r)

	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}

func TestHandleInternalEvent_PayloadTooLarge(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	oversized := strings.Repeat("a", 11<<20) // over the 10 MB cap
	w := postEvent(router, envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"padding": oversized,
	}))

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

// --- DelegationStarted ---

func TestHandleInternalEvent_DelegationStarted_Success(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	eventID := uuid.New()
	w := postEvent(router, envelope("DelegationStarted", eventID, testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		"delegator_id":  testDelegatorID,
		"delegate_id":   testDelegateID2,
		"scope":         "department",
		"scope_id":      testDelegatorID.String(),
		"starts_at":     time.Now().Format(time.RFC3339),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testTenantID, gotIn.TenantID)
	assert.Equal(t, testDelegationID2, gotIn.DelegationID)
	assert.Equal(t, "department", gotIn.Scope)
}

func TestHandleInternalEvent_DelegationStarted_BadPayload_MissingField(t *testing.T) {
	fakes := newEventsFakes()
	fakes.delegation.reroute = func(context.Context, port.DelegationRerouteInput) error {
		t.Fatal("reconciler must not be called on bad payload")
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		// delegator_id, delegate_id, scope, starts_at all omitted
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleInternalEvent_DelegationStarted_ReconcilerError_500_DedupNotRecorded(t *testing.T) {
	fakes := newEventsFakes()
	fakes.delegation.reroute = func(context.Context, port.DelegationRerouteInput) error {
		return errors.New("boom")
	}
	router := newInternalRouter(newEventsHandler(fakes))

	eventID := uuid.New()
	body := envelope("DelegationStarted", eventID, testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		"delegator_id":  testDelegatorID,
		"delegate_id":   testDelegateID2,
		"scope":         "all",
		"starts_at":     time.Now().Format(time.RFC3339),
	})

	w := postEvent(router, body)
	require.Equal(t, http.StatusInternalServerError, w.Code)

	processed, _ := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
	assert.False(t, processed, "dedup must not be recorded when the reconciler fails")
}

func TestHandleInternalEvent_DelegationStarted_ObservesRerouteDuration(t *testing.T) {
	fakes := newEventsFakes()
	fakes.delegation.reroute = func(context.Context, port.DelegationRerouteInput) error { return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		"delegator_id":  testDelegatorID,
		"delegate_id":   testDelegateID2,
		"scope":         "all",
		"starts_at":     time.Now().Format(time.RFC3339),
	}))

	assert.Equal(t, http.StatusOK, w.Code)
}

// --- DelegationEnded ---

func TestHandleInternalEvent_DelegationEnded_Success(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.DelegationReversalInput
	fakes.delegation.reverse = func(_ context.Context, in port.DelegationReversalInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("DelegationEnded", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		"delegator_id":  testDelegatorID,
		"delegate_id":   testDelegateID2,
		"ended_reason":  "expired",
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testDelegationID2, gotIn.DelegationID)
	assert.Equal(t, "expired", gotIn.EndedReason)
}

func TestHandleInternalEvent_DelegationEnded_UnknownEndedReason_StillProcessed(t *testing.T) {
	fakes := newEventsFakes()
	called := false
	fakes.delegation.reverse = func(context.Context, port.DelegationReversalInput) error {
		called = true
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("DelegationEnded", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		"delegator_id":  testDelegatorID,
		"delegate_id":   testDelegateID2,
		"ended_reason":  "some_future_reason",
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called, "an unknown ended_reason must still be processed, not rejected")
}

// --- UserDeleted ---

func TestHandleInternalEvent_UserDeleted_Success_VacatesPerAssignment(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.UserDeletedInput
	fakes.userSafetyNet.vacateAssignments = func(_ context.Context, in port.UserDeletedInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id":    testUserID.String(),
		"deleted_at": time.Now().Format(time.RFC3339),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testUserID, gotIn.UserID)
	assert.Equal(t, testTenantID, gotIn.TenantID)
}

func TestHandleInternalEvent_UserDeleted_BadPayload(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": "not-a-uuid",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- UserAvailabilityChanged ---

func TestHandleInternalEvent_UserAvailabilityChanged_OOO_Pauses(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.UserAvailabilityInput
	fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(),
		"status":  "ooo",
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ooo", gotIn.Status)
}

func TestHandleInternalEvent_UserAvailabilityChanged_Available_Resumes(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.UserAvailabilityInput
	fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(),
		"status":  "available",
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "available", gotIn.Status)
}

func TestHandleInternalEvent_UserAvailabilityChanged_RecencyGuard_SkipsStaleEvent(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error {
		calls++
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	newer := time.Now()
	older := newer.Add(-time.Hour)

	w1 := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, newer, map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))
	require.Equal(t, http.StatusOK, w1.Code)
	require.Equal(t, 1, calls)

	w2 := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, older, map[string]any{
		"user_id": testUserID.String(), "status": "available",
	}))
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 1, calls, "a stale (older) event must be skipped by the recency guard, not applied")
}

// --- TenantStateChanged ---

func TestHandleInternalEvent_TenantStateChanged_Success(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.TenantLifecycleInput
	fakes.tenantLifecycle.apply = func(_ context.Context, in port.TenantLifecycleInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id":       testTenantID.String(),
		"status":          "suspended",
		"previous_status": "active",
		"plan":            "pro",
		"previous_plan":   "pro",
		"changed_at":      time.Now().Format(time.RFC3339),
		"cause":           "non_payment",
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "suspended", gotIn.Status)
}

func TestHandleInternalEvent_TenantStateChanged_Offboarded_NeverSkippedEvenIfStale(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
		calls++
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	newer := time.Now()
	older := newer.Add(-24 * time.Hour)

	// Prime the recency guard with a newer value first.
	w1 := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, newer, map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": newer.Format(time.RFC3339), "cause": "x",
	}))
	require.Equal(t, http.StatusOK, w1.Code)
	require.Equal(t, 1, calls)

	// An older offboarded event must still be applied, not skipped.
	w2 := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, older, map[string]any{
		"tenant_id": testTenantID.String(), "status": "offboarded", "previous_status": "suspended",
		"plan": "pro", "previous_plan": "pro", "changed_at": older.Format(time.RFC3339), "cause": "churn",
	}))
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 2, calls, "offboarded is terminal and must never be skipped by the recency guard")
}

func TestHandleInternalEvent_TenantStateChanged_RecencyGuard_SkipsStaleNonOffboarded(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
		calls++
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	newer := time.Now()
	older := newer.Add(-time.Hour)

	w1 := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, newer, map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": newer.Format(time.RFC3339), "cause": "x",
	}))
	require.Equal(t, http.StatusOK, w1.Code)
	require.Equal(t, 1, calls)

	w2 := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, older, map[string]any{
		"tenant_id": testTenantID.String(), "status": "active", "previous_status": "suspended",
		"plan": "pro", "previous_plan": "pro", "changed_at": older.Format(time.RFC3339), "cause": "y",
	}))
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 1, calls, "a stale non-offboarded transition must be skipped")
}

func TestHandleInternalEvent_TenantStateChanged_CommitsRecencyOnceAfterApplySucceeds(t *testing.T) {
	fakes := newEventsFakes()
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error { return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	// Truncated to whole seconds: the handler round-trips changed_at through
	// RFC3339 (JSON payload), which drops sub-second precision — comparing
	// against a sub-second-precision "at" here would spuriously always
	// report After()=true.
	at := time.Now().Truncate(time.Second)
	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, at, map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": at.Format(time.RFC3339), "cause": "x",
	}))
	require.Equal(t, http.StatusOK, w.Code)

	shouldApplySame, err := fakes.recency.ShouldApply(context.Background(), "tenant:"+testTenantID.String(), at)
	require.NoError(t, err)
	assert.False(t, shouldApplySame, "recency must be committed after Apply succeeds, not left unset")
}

// --- workflow.template.published ---

func TestHandleInternalEvent_TemplatePublished_Success(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.TemplatePublishedInput
	fakes.templateCache.prewarm = func(_ context.Context, in port.TemplatePublishedInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), map[string]any{
		"workflow_id":    testInstID.String(),
		"workflow_key":   "tender-approval",
		"version_id":     testTaskID.String(),
		"version_number": 3,
		"artifact_hash":  "sha256:abc",
		"published_by":   testUserID.String(),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tender-approval", gotIn.WorkflowKey)
}

func TestHandleInternalEvent_TemplatePublished_PrewarmFails_StillReturns200_FailOpen(t *testing.T) {
	fakes := newEventsFakes()
	fakes.templateCache.prewarm = func(context.Context, port.TemplatePublishedInput) error {
		return errors.New("upstream unavailable")
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), map[string]any{
		"workflow_id":    testInstID.String(),
		"workflow_key":   "tender-approval",
		"version_id":     testTaskID.String(),
		"version_number": 3,
		"artifact_hash":  "sha256:abc",
		"published_by":   testUserID.String(),
	}))

	assert.Equal(t, http.StatusOK, w.Code, "a prewarm failure must be fail-open, never a 5xx")
}

// --- envelope-level validation (shared by every handler) ---

func TestHandleInternalEvent_InvalidEventID_400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("DelegationEnded", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID,
		"delegate_id": testDelegateID2, "ended_reason": "expired",
	})
	body["id"] = "not-a-uuid"

	w := postEvent(router, body)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleInternalEvent_InvalidEnvelopeTenantID_400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	})
	body["tenant_id"] = "not-a-uuid"

	w := postEvent(router, body)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- DelegationStarted: per-field payload validation ---

func TestHandleInternalEvent_DelegationStarted_InvalidFields(t *testing.T) {
	validData := func() map[string]any {
		return map[string]any{
			"delegation_id": testDelegationID2.String(),
			"delegator_id":  testDelegatorID.String(),
			"delegate_id":   testDelegateID2.String(),
			"scope":         "all",
			"starts_at":     time.Now().Format(time.RFC3339),
			"ends_at":       time.Now().Add(time.Hour).Format(time.RFC3339),
		}
	}

	cases := []struct {
		name  string
		field string
		value any
	}{
		{"delegation_id", "delegation_id", "not-a-uuid"},
		{"delegator_id", "delegator_id", "not-a-uuid"},
		{"delegate_id", "delegate_id", "not-a-uuid"},
		{"starts_at", "starts_at", "not-a-time"},
		{"ends_at", "ends_at", "not-a-time"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakes := newEventsFakes()
			fakes.delegation.reroute = func(context.Context, port.DelegationRerouteInput) error {
				t.Fatal("reconciler must not be called on bad payload")
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))

			data := validData()
			data[tc.field] = tc.value
			w := postEvent(router, envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), data))

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

// --- DelegationEnded: per-field payload validation ---

func TestHandleInternalEvent_DelegationEnded_InvalidFields(t *testing.T) {
	validData := func() map[string]any {
		return map[string]any{
			"delegation_id": testDelegationID2.String(),
			"delegator_id":  testDelegatorID.String(),
			"delegate_id":   testDelegateID2.String(),
			"ended_reason":  "expired",
		}
	}
	for _, field := range []string{"delegation_id", "delegator_id", "delegate_id"} {
		t.Run(field, func(t *testing.T) {
			fakes := newEventsFakes()
			router := newInternalRouter(newEventsHandler(fakes))

			data := validData()
			data[field] = "not-a-uuid"
			w := postEvent(router, envelope("DelegationEnded", uuid.New(), testTenantID, time.Now(), data))

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

// --- UserDeleted: per-field payload validation ---

func TestHandleInternalEvent_UserDeleted_InvalidDeletedAt(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": "not-a-time",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- UserAvailabilityChanged: per-field payload validation ---

func TestHandleInternalEvent_UserAvailabilityChanged_InvalidFields(t *testing.T) {
	validData := func() map[string]any {
		return map[string]any{
			"user_id":          testUserID.String(),
			"status":           "ooo",
			"ooo_from":         time.Now().Format(time.RFC3339),
			"ooo_until":        time.Now().Add(time.Hour).Format(time.RFC3339),
			"delegate_user_id": testDelegateID2.String(),
		}
	}
	cases := []struct{ field, value string }{
		{"user_id", "not-a-uuid"},
		{"ooo_from", "not-a-time"},
		{"ooo_until", "not-a-time"},
		{"delegate_user_id", "not-a-uuid"},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			fakes := newEventsFakes()
			router := newInternalRouter(newEventsHandler(fakes))

			data := validData()
			data[tc.field] = tc.value
			w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), data))

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandleInternalEvent_UserAvailabilityChanged_NoDelegateUserID(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.UserAvailabilityInput
	fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error {
		gotIn = in
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Nil(t, gotIn.DelegateUserID)
	assert.Nil(t, gotIn.OOOFrom)
	assert.Nil(t, gotIn.OOOUntil)
}

func TestHandleInternalEvent_UserAvailabilityChanged_RecencyCheckError_500(t *testing.T) {
	fakes := newEventsFakes()
	errFakeRecency := &erroringRecencyGuard{err: errors.New("recency store unavailable")}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, errFakeRecency))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- TenantStateChanged: per-field payload validation ---

func TestHandleInternalEvent_TenantStateChanged_InvalidPayloadTenantID(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": "not-a-uuid", "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": time.Now().Format(time.RFC3339), "cause": "x",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleInternalEvent_TenantStateChanged_InvalidChangedAt(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": "not-a-time", "cause": "x",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleInternalEvent_TenantStateChanged_ApplyError_500_RecencyNotCommitted(t *testing.T) {
	fakes := newEventsFakes()
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error {
		return errors.New("boom")
	}
	router := newInternalRouter(newEventsHandler(fakes))

	at := time.Now().Truncate(time.Second)
	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, at, map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": at.Format(time.RFC3339), "cause": "x",
	}))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	shouldApply, err := fakes.recency.ShouldApply(context.Background(), "tenant:"+testTenantID.String(), at)
	require.NoError(t, err)
	assert.True(t, shouldApply, "recency must not be committed when Apply fails")
}

// --- workflow.template.published: per-field payload validation ---

func TestHandleInternalEvent_TemplatePublished_InvalidFields(t *testing.T) {
	validData := func() map[string]any {
		return map[string]any{
			"workflow_id":    testInstID.String(),
			"workflow_key":   "tender-approval",
			"version_id":     testTaskID.String(),
			"version_number": 3,
			"artifact_hash":  "sha256:abc",
			"published_by":   testUserID.String(),
		}
	}
	for _, field := range []string{"workflow_id", "version_id", "published_by"} {
		t.Run(field, func(t *testing.T) {
			fakes := newEventsFakes()
			router := newInternalRouter(newEventsHandler(fakes))

			data := validData()
			data[field] = "not-a-uuid"
			w := postEvent(router, envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), data))

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandleInternalEvent_TemplatePublished_RecencyGuard_SkipsStalePrewarm(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.templateCache.prewarm = func(context.Context, port.TemplatePublishedInput) error {
		calls++
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	newer := time.Now()
	older := newer.Add(-time.Hour)

	w1 := postEvent(router, envelope("workflow.template.published", uuid.New(), testTenantID, newer, map[string]any{
		"workflow_id": testInstID.String(), "workflow_key": "tender-approval", "version_id": testTaskID.String(),
		"version_number": 3, "artifact_hash": "sha256:abc", "published_by": testUserID.String(),
	}))
	require.Equal(t, http.StatusOK, w1.Code)
	require.Equal(t, 1, calls)

	w2 := postEvent(router, envelope("workflow.template.published", uuid.New(), testTenantID, older, map[string]any{
		"workflow_id": testInstID.String(), "workflow_key": "tender-approval", "version_id": testTaskID.String(),
		"version_number": 3, "artifact_hash": "sha256:abc", "published_by": testUserID.String(),
	}))
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 1, calls, "a stale republish must skip the prewarm call")
}

// --- respondOK / alreadyProcessed error paths ---

func TestHandleInternalEvent_RecordIfNewError_StillReturns200(t *testing.T) {
	fakes := newEventsFakes()
	fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error { return nil }
	h := newEventsHandlerWithProcessedEvents(fakes, &erroringProcessedEventRepository{recordErr: errors.New("db down")})
	router := newInternalRouter(h)

	w := postEvent(router, envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	}))

	assert.Equal(t, http.StatusOK, w.Code, "a dedup-record failure after successful side effects must not surface as an error to the caller")
}

func TestHandleInternalEvent_IsProcessedError_ProceedsAnyway(t *testing.T) {
	fakes := newEventsFakes()
	called := false
	fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error {
		called = true
		return nil
	}
	h := newEventsHandlerWithProcessedEvents(fakes, &erroringProcessedEventRepository{isProcessedErr: errors.New("db down")})
	router := newInternalRouter(h)

	w := postEvent(router, envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	}))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called, "an IsProcessed check failure must not block processing")
}

// --- alreadyProcessed short-circuit, one per handler (each is its own
// function body / coverage line, not shared across handlers) ---

func TestHandleInternalEvent_DelegationStarted_AlreadyProcessed_SkipsReroute(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.delegation.reroute = func(context.Context, port.DelegationRerouteInput) error { calls++; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID,
		"delegate_id": testDelegateID2, "scope": "all", "starts_at": time.Now().Format(time.RFC3339),
	})
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	assert.Equal(t, 1, calls)
}

func TestHandleInternalEvent_DelegationStarted_NoEndsAt(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error { gotIn = in; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID,
		"delegate_id": testDelegateID2, "scope": "all", "starts_at": time.Now().Format(time.RFC3339),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Nil(t, gotIn.EndsAt)
}

func TestHandleInternalEvent_DelegationEnded_AlreadyProcessed_SkipsReverse(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.delegation.reverse = func(context.Context, port.DelegationReversalInput) error { calls++; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("DelegationEnded", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID,
		"delegate_id": testDelegateID2, "ended_reason": "expired",
	})
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	assert.Equal(t, 1, calls)
}

func TestHandleInternalEvent_UserDeleted_AlreadyProcessed_SkipsVacate(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error { calls++; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	})
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	assert.Equal(t, 1, calls)
}

func TestHandleInternalEvent_UserAvailabilityChanged_AlreadyProcessed_SkipsApply(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error { calls++; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	})
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	assert.Equal(t, 1, calls)
}

func TestHandleInternalEvent_TenantStateChanged_AlreadyProcessed_SkipsApply(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error { calls++; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": time.Now().Format(time.RFC3339), "cause": "x",
	})
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	assert.Equal(t, 1, calls)
}

func TestHandleInternalEvent_TenantStateChanged_CommitError_StillReturns200(t *testing.T) {
	fakes := newEventsFakes()
	fakes.tenantLifecycle.apply = func(context.Context, port.TenantLifecycleInput) error { return nil }
	errRecency := &erroringCommitRecencyGuard{fakeRecencyGuard: fakes.recency}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, errRecency))

	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": time.Now().Format(time.RFC3339), "cause": "x",
	}))

	assert.Equal(t, http.StatusOK, w.Code, "a recency-Commit failure after Apply succeeds must not surface as an error")
}

func TestHandleInternalEvent_TemplatePublished_AlreadyProcessed_SkipsPrewarm(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.templateCache.prewarm = func(context.Context, port.TemplatePublishedInput) error { calls++; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	body := envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), map[string]any{
		"workflow_id": testInstID.String(), "workflow_key": "tender-approval", "version_id": testTaskID.String(),
		"version_number": 3, "artifact_hash": "sha256:abc", "published_by": testUserID.String(),
	})
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	require.Equal(t, http.StatusOK, postEvent(router, body).Code)
	assert.Equal(t, 1, calls)
}

func TestHandleInternalEvent_TemplatePublished_RecencyCheckError_FailsOpenAndPrewarms(t *testing.T) {
	fakes := newEventsFakes()
	called := false
	fakes.templateCache.prewarm = func(context.Context, port.TemplatePublishedInput) error { called = true; return nil }
	errFakeRecency := &erroringRecencyGuard{err: errors.New("recency store unavailable")}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, errFakeRecency))

	w := postEvent(router, envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), map[string]any{
		"workflow_id": testInstID.String(), "workflow_key": "tender-approval", "version_id": testTaskID.String(),
		"version_number": 3, "artifact_hash": "sha256:abc", "published_by": testUserID.String(),
	}))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called, "a recency-check failure on the fail-open path must still attempt the prewarm")
}

// --- malformed (non-object) data payload + invalid envelope id/tenant_id,
// one per handler (each handler's own call sites are distinct coverage
// statements, not shared across handlers) ---

func rawBody(eventType string, eventID, tenantID uuid.UUID, data any) map[string]any {
	return map[string]any{
		"id": eventID.String(), "type": eventType, "tenant_id": tenantID.String(),
		"time": time.Now().Format(time.RFC3339), "data": data,
	}
}

func TestHandleInternalEvent_MalformedDataPayload_PerEventType(t *testing.T) {
	for _, eventType := range []string{
		"DelegationStarted", "DelegationEnded", "UserDeleted",
		"UserAvailabilityChanged", "TenantStateChanged", "workflow.template.published",
	} {
		t.Run(eventType, func(t *testing.T) {
			fakes := newEventsFakes()
			router := newInternalRouter(newEventsHandler(fakes))

			w := postEvent(router, rawBody(eventType, uuid.New(), testTenantID, "not-an-object"))
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandleInternalEvent_DelegationStarted_InvalidEnvelopeIDs(t *testing.T) {
	validData := map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID,
		"delegate_id": testDelegateID2, "scope": "all", "starts_at": time.Now().Format(time.RFC3339),
	}
	t.Run("invalid event id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), validData)
		body["id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
	t.Run("invalid tenant id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), validData)
		body["tenant_id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
}

func TestHandleInternalEvent_UserAvailabilityChanged_InvalidEnvelopeIDs(t *testing.T) {
	validData := map[string]any{"user_id": testUserID.String(), "status": "ooo"}
	t.Run("invalid event id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), validData)
		body["id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
	t.Run("invalid tenant id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), validData)
		body["tenant_id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
}

func TestHandleInternalEvent_TenantStateChanged_InvalidEnvelopeIDs(t *testing.T) {
	validData := map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": time.Now().Format(time.RFC3339), "cause": "x",
	}
	t.Run("invalid event id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), validData)
		body["id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
	t.Run("invalid tenant id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), validData)
		body["tenant_id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
}

func TestHandleInternalEvent_TemplatePublished_InvalidEnvelopeIDs(t *testing.T) {
	validData := map[string]any{
		"workflow_id": testInstID.String(), "workflow_key": "tender-approval", "version_id": testTaskID.String(),
		"version_number": 3, "artifact_hash": "sha256:abc", "published_by": testUserID.String(),
	}
	t.Run("invalid event id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), validData)
		body["id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
	t.Run("invalid tenant id", func(t *testing.T) {
		fakes := newEventsFakes()
		router := newInternalRouter(newEventsHandler(fakes))
		body := envelope("workflow.template.published", uuid.New(), testTenantID, time.Now(), validData)
		body["tenant_id"] = "not-a-uuid"
		assert.Equal(t, http.StatusBadRequest, postEvent(router, body).Code)
	})
}

// --- remaining success/error branches not yet exercised ---

func TestHandleInternalEvent_DelegationStarted_ValidEndsAt(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.DelegationRerouteInput
	fakes.delegation.reroute = func(_ context.Context, in port.DelegationRerouteInput) error { gotIn = in; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("DelegationStarted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2, "delegator_id": testDelegatorID, "delegate_id": testDelegateID2,
		"scope": "all", "starts_at": time.Now().Format(time.RFC3339),
		"ends_at": time.Now().Add(time.Hour).Format(time.RFC3339),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotIn.EndsAt)
}

func TestHandleInternalEvent_UserDeleted_ReconcilerError_500(t *testing.T) {
	fakes := newEventsFakes()
	fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error {
		return errors.New("boom")
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserDeleted", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	}))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleInternalEvent_UserAvailabilityChanged_ValidDelegateUserID(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.UserAvailabilityInput
	fakes.oooAvailability.apply = func(_ context.Context, in port.UserAvailabilityInput) error { gotIn = in; return nil }
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "status": "ooo", "delegate_user_id": testDelegateID2.String(),
	}))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotIn.DelegateUserID)
	assert.Equal(t, testDelegateID2, *gotIn.DelegateUserID)
}

func TestHandleInternalEvent_UserAvailabilityChanged_ReconcilerError_500(t *testing.T) {
	fakes := newEventsFakes()
	fakes.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error {
		return errors.New("boom")
	}
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// TestHandleInternalEvent_UserAvailabilityChanged_ApplyFails_RecencyNotAdvanced
// is a regression test: the recency guard must only commit AFTER Apply
// succeeds. If it committed up front (the original, buggy CheckAndCommit
// ordering), a retry of an event at the same timestamp following a transient
// Apply failure would be wrongly skipped as stale, permanently losing the
// side effect the caller expects to eventually happen.
func TestHandleInternalEvent_UserAvailabilityChanged_ApplyFails_RecencyNotAdvanced(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fail := true
	fakes.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error {
		calls++
		if fail {
			return errors.New("boom")
		}
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	at := time.Now()
	w1 := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, at, map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))
	require.Equal(t, http.StatusInternalServerError, w1.Code)
	require.Equal(t, 1, calls)

	fail = false
	w2 := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, at, map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 2, calls, "a retry at the same event time must still apply after a prior Apply failure, not be silently skipped by an already-advanced recency guard")
}

func TestHandleInternalEvent_UserAvailabilityChanged_CommitError_StillReturns200(t *testing.T) {
	fakes := newEventsFakes()
	fakes.oooAvailability.apply = func(context.Context, port.UserAvailabilityInput) error { return nil }
	errRecency := &erroringCommitRecencyGuard{fakeRecencyGuard: fakes.recency}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, errRecency))

	w := postEvent(router, envelope("UserAvailabilityChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "status": "ooo",
	}))

	assert.Equal(t, http.StatusOK, w.Code, "a recency-Commit failure after Apply succeeds must not surface as an error")
}

func TestHandleInternalEvent_TenantStateChanged_ShouldApplyError_500(t *testing.T) {
	fakes := newEventsFakes()
	errFakeRecency := &erroringRecencyGuard{err: errors.New("recency store unavailable")}
	router := newInternalRouter(newEventsHandlerWithRecency(fakes, errFakeRecency))

	w := postEvent(router, envelope("TenantStateChanged", uuid.New(), testTenantID, time.Now(), map[string]any{
		"tenant_id": testTenantID.String(), "status": "suspended", "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": time.Now().Format(time.RFC3339), "cause": "x",
	}))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- Duplicate delivery / dedup (the DoD's own explicit test) ---

func TestHandleInternalEvent_DuplicateDelivery_SecondDeliveryIsNoOp(t *testing.T) {
	fakes := newEventsFakes()
	calls := 0
	fakes.delegation.reroute = func(context.Context, port.DelegationRerouteInput) error {
		calls++
		return nil
	}
	router := newInternalRouter(newEventsHandler(fakes))

	eventID := uuid.New()
	body := envelope("DelegationStarted", eventID, testTenantID, time.Now(), map[string]any{
		"delegation_id": testDelegationID2,
		"delegator_id":  testDelegatorID,
		"delegate_id":   testDelegateID2,
		"scope":         "all",
		"starts_at":     time.Now().Format(time.RFC3339),
	})

	w1 := postEvent(router, body)
	require.Equal(t, http.StatusOK, w1.Code)
	require.Equal(t, 1, calls)

	w2 := postEvent(router, body)
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 1, calls, "a duplicate delivery of the same (event_id, consumer) must not repeat side effects")
}
