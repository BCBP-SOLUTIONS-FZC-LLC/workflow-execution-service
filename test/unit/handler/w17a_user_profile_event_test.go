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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW17aVacateFailed = errors.New("w17a: pq: could not serialize access: tenant 4f2 row 77")
	errW17aGuardDown    = errors.New("w17a: recency store read failed: host=10.9.8.7")
)

// w17aRefuseOnUserProfile posts body to the user-profile route and checks the 400 problem
// body, the bad-payload count under label, and that no reconciler, guard or inbox write ran.
func w17aRefuseOnUserProfile(t *testing.T, label, body, wantDetail string, leaked ...string) {
	t.Helper()
	obstest.Init(t)
	rig := w17aNewRig()
	counted := map[string]string{"event_type": label, "result": "bad_payload"}
	before := obstest.Sum(t, w17aIngestMetric, counted)

	w := rig.post(w17aUserProfileRoute, body)

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	p := w17aDecodeProblem(t, w)
	assert.Equal(t, http.StatusBadRequest, p.Status)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, w17aBadRequestType, p.Type)
	assert.Equal(t, "Bad Request", p.Title)
	assert.Equal(t, wantDetail, p.Detail)
	for _, text := range leaked {
		assert.NotContains(t, w.Body.String(), text, "the request text is not echoed")
	}
	assert.True(t, rig.untouched(), "no reconciler, guard or inbox write ran")
	assert.Empty(t, rig.inbox.lookups)
	assert.Equal(t, before+1, obstest.Sum(t, w17aIngestMetric, counted))
}

func TestUserProfileEvent_UserDeletedForAUserWithNoAssignments_IsAcknowledgedAndMarked(t *testing.T) {
	obstest.Init(t)
	rig := w17aNewRig()
	tenantID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	succeeded := map[string]string{"event_type": "UserDeleted", "result": "ok"}
	failed := map[string]string{"event_type": "UserDeleted", "result": "error"}
	okBefore, failedBefore := obstest.Sum(t, w17aIngestMetric, succeeded), obstest.Sum(t, w17aIngestMetric, failed)

	w := rig.postEnvelope(w17aUserProfileRoute, envelope("UserDeleted", eventID, tenantID, time.Now(), w17aUserDeletedData(userID)))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Empty(t, w.Body.String())
	assert.Equal(t, []w17aVacateCall{{tenantID: tenantID, userID: userID, gucTenant: tenantID.String()}}, rig.assignments.calls,
		"the vacate ran once for the event's tenant and user and found nothing")
	assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.lookups)
	assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.marks, "an event that changed nothing is still marked processed")
	assert.Empty(t, rig.log.at("warn"))
	assert.Empty(t, rig.log.at("error"))
	assert.Equal(t, okBefore+1, obstest.Sum(t, w17aIngestMetric, succeeded))
	assert.Equal(t, failedBefore, obstest.Sum(t, w17aIngestMetric, failed))
}

func TestUserProfileEvent_TypeThisRouteDoesNotHandle_IsAcknowledgedAndIgnored(t *testing.T) {
	obstest.Init(t)
	unhandled := map[string]string{"event_type": "unknown", "result": "unhandled_type"}
	cases := []struct {
		name      string
		eventType string
		data      map[string]any
	}{
		{name: "a delegation event", eventType: "DelegationStarted", data: w17aStartedData(uuid.New(), uuid.New(), uuid.New())},
		{name: "a tenant event", eventType: "TenantStateChanged", data: map[string]any{"status": "active"}},
		{name: "a lowercase spelling of a handled type", eventType: "userdeleted", data: w17aUserDeletedData(uuid.New())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w17aNewRig()
			before := obstest.Sum(t, w17aIngestMetric, unhandled)
			ownLabelBefore := obstest.Sum(t, w17aIngestMetric, map[string]string{"event_type": tc.eventType})

			w := rig.postEnvelope(w17aUserProfileRoute, envelope(tc.eventType, uuid.New(), uuid.New(), time.Now(), tc.data))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Empty(t, w.Body.String())
			assert.True(t, rig.untouched(), "nothing ran for a type this route does not take")
			assert.Empty(t, rig.inbox.lookups)
			assert.Equal(t, before+1, obstest.Sum(t, w17aIngestMetric, unhandled))
			assert.Equal(t, ownLabelBefore, obstest.Sum(t, w17aIngestMetric, map[string]string{"event_type": tc.eventType}), "the type's own label does not move")
			warnings := rig.log.at("warn")
			require.Len(t, warnings, 1)
			assert.Equal(t, "internal events: ignoring unhandled type", warnings[0].msg)
			assert.Equal(t, tc.eventType, warnings[0].fields["event_type"])
		})
	}
}

func TestUserProfileEvent_UserDeletedVacateFails_Answers500AndTheRedeliveryIsHandled(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name string
		err  error
	}{
		{name: "the update fails with a database error", err: fmt.Errorf("vacate assignments: %w", errW17aVacateFailed)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w17aNewRig()
			rig.assignments.results = []error{tc.err}
			tenantID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
			env := envelope("UserDeleted", eventID, tenantID, time.Now(), w17aUserDeletedData(userID))
			wantCall := w17aVacateCall{tenantID: tenantID, userID: userID, gucTenant: tenantID.String()}
			failed := map[string]string{"event_type": "UserDeleted", "result": "error"}
			succeeded := map[string]string{"event_type": "UserDeleted", "result": "ok"}
			failedBefore, okBefore := obstest.Sum(t, w17aIngestMetric, failed), obstest.Sum(t, w17aIngestMetric, succeeded)

			w := rig.postEnvelope(w17aUserProfileRoute, env)

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			p := w17aDecodeProblem(t, w)
			assert.Equal(t, http.StatusInternalServerError, p.Status)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, w17aInternalType, p.Type)
			assert.Equal(t, "Internal Server Error", p.Title)
			assert.Equal(t, "failed to process event", p.Detail)
			assert.NotContains(t, w.Body.String(), "serialize", "the error text is not echoed")
			assert.NotContains(t, w.Body.String(), "tenant 4f2")
			assert.Equal(t, []w17aVacateCall{wantCall}, rig.assignments.calls)
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.lookups)
			assert.Empty(t, rig.inbox.marks, "the event is not marked processed, so it is delivered again")
			assert.Equal(t, failedBefore+1, obstest.Sum(t, w17aIngestMetric, failed))
			assert.Equal(t, okBefore, obstest.Sum(t, w17aIngestMetric, succeeded))
			logged := rig.log.at("error")
			require.Len(t, logged, 1)
			assert.Equal(t, "UserDeleted", logged[0].fields["event_type"])

			w = rig.postEnvelope(w17aUserProfileRoute, env)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Equal(t, []w17aVacateCall{wantCall, wantCall}, rig.assignments.calls, "the redelivery vacates for the same tenant and user")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.marks)
		})
	}
}

func TestUserProfileEvent_AvailabilityGuardReadFailsInsideTheLock_Answers500AndAppliesNothing(t *testing.T) {
	obstest.Init(t)
	rig := w17aNewRig()
	rig.recency.shouldApplyErr = errW17aGuardDown
	tenantID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Second)
	failed := map[string]string{"event_type": "UserAvailabilityChanged", "result": "error"}
	failedBefore := obstest.Sum(t, w17aIngestMetric, failed)

	w := rig.postEnvelope(w17aUserProfileRoute, envelope("UserAvailabilityChanged", eventID, tenantID, at, map[string]any{
		"user_id": userID.String(), "status": "ooo",
	}))

	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	p := w17aDecodeProblem(t, w)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, w17aInternalType, p.Type)
	assert.Equal(t, "failed to process event", p.Detail)
	assert.NotContains(t, w.Body.String(), "10.9.8.7", "the error text is not echoed")
	assert.NotContains(t, w.Body.String(), "recency store")
	require.Len(t, rig.recency.locks, 1, "the lock was taken, so the failure is the read inside it")
	require.Len(t, rig.recency.shouldApply, 1)
	assert.Equal(t, rig.recency.locks[0], rig.recency.shouldApply[0].scopeKey, "the read is for the lock's own scope")
	assert.Contains(t, rig.recency.locks[0], tenantID.String())
	assert.Contains(t, rig.recency.locks[0], userID.String())
	assert.True(t, at.Equal(rig.recency.shouldApply[0].at))
	assert.Empty(t, rig.availability.calls, "nothing is applied")
	assert.Empty(t, rig.recency.commits, "no recency is recorded")
	assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.lookups)
	assert.Empty(t, rig.inbox.marks, "the event is not marked processed")
	assert.Equal(t, failedBefore+1, obstest.Sum(t, w17aIngestMetric, failed))
}

func TestUserProfileEvent_OneEnvelopeIDForTwoTypes_TheSecondIsAlreadyProcessed(t *testing.T) {
	obstest.Init(t)
	tenantID, userID := uuid.New(), uuid.New()
	deleted := func(id uuid.UUID, at time.Time) map[string]any {
		return envelope("UserDeleted", id, tenantID, at, w17aUserDeletedData(userID))
	}
	availability := func(id uuid.UUID, at time.Time) map[string]any {
		return envelope("UserAvailabilityChanged", id, tenantID, at, map[string]any{"user_id": userID.String(), "status": "ooo"})
	}
	cases := []struct {
		name             string
		first            func(uuid.UUID, time.Time) map[string]any
		second           func(uuid.UUID, time.Time) map[string]any
		secondType       string
		wantVacates      int
		wantAvailability int
	}{
		{name: "a user deletion then an availability change", first: deleted, second: availability, secondType: "UserAvailabilityChanged", wantVacates: 1},
		{name: "an availability change then a user deletion", first: availability, second: deleted, secondType: "UserDeleted", wantAvailability: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w17aNewRig()
			eventID := uuid.New()
			now := time.Now().UTC().Truncate(time.Second)

			first := rig.postEnvelope(w17aUserProfileRoute, tc.first(eventID, now))
			require.Equal(t, http.StatusOK, first.Code, first.Body.String())
			require.Len(t, rig.assignments.calls, tc.wantVacates)
			require.Len(t, rig.availability.calls, tc.wantAvailability)
			require.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.marks)
			secondType := map[string]string{"event_type": tc.secondType, "result": "ok"}
			secondBefore := obstest.Sum(t, w17aIngestMetric, secondType)

			second := rig.postEnvelope(w17aUserProfileRoute, tc.second(eventID, now.Add(time.Hour)))

			require.Equal(t, http.StatusOK, second.Code, second.Body.String())
			assert.Empty(t, second.Body.String())
			assert.Len(t, rig.assignments.calls, tc.wantVacates, "the second event vacated nothing")
			assert.Len(t, rig.availability.calls, tc.wantAvailability, "the second event applied nothing")
			assert.Len(t, rig.recency.commits, tc.wantAvailability, "the second event recorded no recency")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}, {eventID, port.ConsumerUser}}, rig.inbox.lookups,
				"both events were looked up under the same id and consumer")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerUser}}, rig.inbox.marks, "only the first was marked")
			assert.Equal(t, secondBefore+1, obstest.Sum(t, w17aIngestMetric, secondType))
		})
	}
}

func TestUserProfileEvent_MalformedEnvelope_Answers400AndRunsNoHandler(t *testing.T) {
	noType := envelope("", uuid.New(), uuid.New(), time.Now(), map[string]any{})
	noTimestamp := envelope("UserDeleted", uuid.New(), uuid.New(), time.Now(), w17aUserDeletedData(uuid.New()))
	delete(noTimestamp, "time")
	zeroTimestamp := envelope("UserDeleted", uuid.New(), uuid.New(), time.Time{}, w17aUserDeletedData(uuid.New()))
	missingType := envelope("UserDeleted", uuid.New(), uuid.New(), time.Now(), w17aUserDeletedData(uuid.New()))
	delete(missingType, "type")
	cases := []struct {
		name       string
		body       string
		wantDetail string
		leaked     string
	}{
		{name: "text that is not JSON", body: "this-is-not-json", wantDetail: "request body is not valid JSON", leaked: "this-is-not-json"},
		{name: "an empty body", body: "", wantDetail: "request body is not valid JSON"},
		{name: "an envelope without a type", body: w17aMarshal(t, missingType), wantDetail: "missing event type"},
		{name: "an envelope with an empty type", body: w17aMarshal(t, noType), wantDetail: "missing event type"},
		{name: "an envelope without a timestamp", body: w17aMarshal(t, noTimestamp), wantDetail: "missing or zero envelope timestamp"},
		{name: "an envelope with a zero timestamp", body: w17aMarshal(t, zeroTimestamp), wantDetail: "missing or zero envelope timestamp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var leaked []string
			if tc.leaked != "" {
				leaked = append(leaked, tc.leaked)
			}
			w17aRefuseOnUserProfile(t, "unknown", tc.body, tc.wantDetail, leaked...)
		})
	}
}

func TestUserProfileEvent_UserDeletedEnvelopeIDsNotUUIDs_Answer400(t *testing.T) {
	cases := []struct {
		name       string
		field      string
		value      string
		wantDetail string
	}{
		{name: "an event id that is not a UUID", field: "id", value: "not-a-uuid", wantDetail: "invalid event id"},
		{name: "an empty event id", field: "id", value: "", wantDetail: "invalid event id"},
		{name: "a tenant id that is not a UUID", field: "tenant_id", value: "not-a-uuid", wantDetail: "invalid tenant_id"},
		{name: "an empty tenant id", field: "tenant_id", value: "", wantDetail: "invalid tenant_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := envelope("UserDeleted", uuid.New(), uuid.New(), time.Now(), w17aUserDeletedData(uuid.New()))
			env[tc.field] = tc.value
			w17aRefuseOnUserProfile(t, "UserDeleted", w17aMarshal(t, env), tc.wantDetail)
		})
	}
}

func TestUserProfileEvent_UserDeletedUserIDMissingOrNotAUUID_Answers400(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(data map[string]any)
	}{
		{name: "no user_id", mutate: func(data map[string]any) { delete(data, "user_id") }},
		{name: "a null user_id", mutate: func(data map[string]any) { data["user_id"] = nil }},
		{name: "an empty user_id", mutate: func(data map[string]any) { data["user_id"] = "" }},
		{name: "a user_id that is not a UUID", mutate: func(data map[string]any) { data["user_id"] = "not-a-uuid" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := w17aUserDeletedData(uuid.New())
			tc.mutate(data)
			env := envelope("UserDeleted", uuid.New(), uuid.New(), time.Now(), data)
			w17aRefuseOnUserProfile(t, "UserDeleted", w17aMarshal(t, env), "invalid user_id in payload")
		})
	}
}

func TestUserProfileEvent_UserDeletedDeletedAtMissingOrNotRFC3339_Answers400(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(data map[string]any)
	}{
		{name: "no deleted_at", mutate: func(data map[string]any) { delete(data, "deleted_at") }},
		{name: "a null deleted_at", mutate: func(data map[string]any) { data["deleted_at"] = nil }},
		{name: "an empty deleted_at", mutate: func(data map[string]any) { data["deleted_at"] = "" }},
		{name: "a deleted_at that is not a time", mutate: func(data map[string]any) { data["deleted_at"] = "not-a-time" }},
		{name: "a deleted_at that is only a date", mutate: func(data map[string]any) { data["deleted_at"] = "2030-01-02" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := w17aUserDeletedData(uuid.New())
			tc.mutate(data)
			env := envelope("UserDeleted", uuid.New(), uuid.New(), time.Now(), data)
			w17aRefuseOnUserProfile(t, "UserDeleted", w17aMarshal(t, env), "invalid deleted_at in payload")
		})
	}
}

func TestUserProfileEvent_MembershipRevokedBadFields_Answers400AndVacatesNothing(t *testing.T) {
	revoked := func() map[string]any {
		return envelope("MembershipRevoked", uuid.New(), uuid.New(), time.Now(), map[string]any{"user_id": uuid.NewString()})
	}
	cases := []struct {
		name       string
		mutate     func(env map[string]any)
		wantDetail string
	}{
		{name: "a payload that is not an object", mutate: func(env map[string]any) { env["data"] = "not-an-object" }, wantDetail: "invalid MembershipRevoked payload"},
		{name: "an event id that is not a UUID", mutate: func(env map[string]any) { env["id"] = "not-a-uuid" }, wantDetail: "invalid event id"},
		{name: "a tenant id that is not a UUID", mutate: func(env map[string]any) { env["tenant_id"] = "not-a-uuid" }, wantDetail: "invalid tenant_id"},
		{name: "an empty tenant id", mutate: func(env map[string]any) { env["tenant_id"] = "" }, wantDetail: "invalid tenant_id"},
		{name: "a user_id that is not a UUID", mutate: func(env map[string]any) { env["data"] = map[string]any{"user_id": "not-a-uuid"} }, wantDetail: "invalid user_id in payload"},
		{name: "no user_id", mutate: func(env map[string]any) { env["data"] = map[string]any{} }, wantDetail: "invalid user_id in payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := revoked()
			tc.mutate(env)
			w17aRefuseOnUserProfile(t, "MembershipRevoked", w17aMarshal(t, env), tc.wantDetail)
		})
	}
}
