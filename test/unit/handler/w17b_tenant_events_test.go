package handler_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errW17bRecencyRead = errors.New("w17b: recency read failed: password=hunter2")
	errW17bInboxRead   = errors.New("w17b: inbox read failed")
	errW17bInboxWrite  = errors.New("w17b: inbox write failed")
)

// An offboarding is read with the equal-time-tolerant recency check; when that read fails the answer is a
// generic 500 (so the sender retries), and nothing is applied, committed or recorded.
func TestTenantEvents_OffboardingRecencyReadFails_Returns500AndAppliesNothing(t *testing.T) {
	cases := []struct {
		name string
		body func(eventID, tenantID uuid.UUID) map[string]any
	}{
		{"TenantStateChanged offboarded", func(eventID, tenantID uuid.UUID) map[string]any {
			return envelope("TenantStateChanged", eventID, tenantID, w17bEventTime, w17bTenantStatePayload(tenantID, "offboarded", w17bEventTime))
		}},
		{"TenantMembershipsPurged", func(eventID, tenantID uuid.UUID) map[string]any {
			return envelope("TenantMembershipsPurged", eventID, tenantID, w17bEventTime, map[string]any{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW17bTenantRig()
			rig.recency.unlessOlderErr = errW17bRecencyRead
			tenantID, eventID := uuid.New(), uuid.New()

			w := w17bPost(rig.router, w17bTenantPath, tc.body(eventID, tenantID))

			p := w17bDecodeProblem(t, w, http.StatusInternalServerError)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, "failed to process event", p.Detail)
			assert.NotContains(t, w.Body.String(), "hunter2", "the read failure text never reaches the sender")
			scope := "tenant:" + tenantID.String()
			assert.Equal(t, []string{scope}, rig.recency.locked)
			assert.Equal(t, []string{"ShouldApplyUnlessOlder"}, rig.recency.methods(), "an offboarding uses the equal-time-tolerant read and commits nothing after it fails")
			assert.Equal(t, scope, rig.recency.calls[0].Scope)
			assert.True(t, w17bEventTime.Equal(rig.recency.calls[0].At))
			assert.Empty(t, rig.lifecycle.inputs, "nothing is applied")
			assert.Equal(t, []w17bInboxCall{{eventID, w17bMembershipConsumer}}, rig.inbox.checked)
			assert.Empty(t, rig.inbox.marked, "a failed event stays redeliverable")
		})
	}
}

// A purge signal is an offboarding of the envelope tenant: status offboarded, the earlier status active,
// stamped with the event's own time, applied under that tenant, and recorded as handled.
func TestTenantEvents_MembershipsPurged_AppliesAnOffboardingForTheEnvelopeTenant(t *testing.T) {
	cases := []struct {
		name string
		data func(tenantID uuid.UUID) any
	}{
		{"empty object", func(uuid.UUID) any { return map[string]any{} }},
		{"no payload", func(uuid.UUID) any { return nil }},
		{"payload naming the same tenant", func(tenantID uuid.UUID) any { return map[string]any{"tenant_id": tenantID.String()} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW17bTenantRig()
			tenantID, eventID := uuid.New(), uuid.New()
			body := envelope("TenantMembershipsPurged", eventID, tenantID, w17bEventTime, nil)
			body["data"] = tc.data(tenantID)

			w := w17bPost(rig.router, w17bTenantPath, body)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, rig.lifecycle.inputs, 1)
			got := rig.lifecycle.inputs[0]
			assert.Equal(t, tenantID, got.TenantID)
			assert.Equal(t, "offboarded", got.Status)
			assert.Equal(t, "active", got.PreviousStatus)
			assert.True(t, w17bEventTime.Equal(got.ChangedAt), "changed_at is the event time, got %s", got.ChangedAt)
			assert.Empty(t, got.Plan)
			assert.Empty(t, got.PreviousPlan)
			assert.Equal(t, []string{tenantID.String()}, rig.lifecycle.ctxTenants)
			assert.Equal(t, []w17bInboxCall{{eventID, w17bMembershipConsumer}}, rig.inbox.marked)
			require.Len(t, rig.recency.calls, 2)
			assert.Equal(t, "Commit", rig.recency.calls[1].Method)
			assert.Equal(t, "tenant:"+tenantID.String(), rig.recency.calls[1].Scope)
			assert.True(t, w17bEventTime.Equal(rig.recency.calls[1].At))
		})
	}
}

// A schema-registry framed payload on the tenant route is decoded and reconciled like a plain one.
func TestTenantEvents_TenantStateChanged_SchemaRegistryFrame_IsReconciledLikeAPlainPayload(t *testing.T) {
	tenantID, eventID := uuid.New(), uuid.New()
	payload := w17bTenantStatePayload(tenantID, "suspended", w17bEventTime)
	payload["previous_plan"] = "basic"
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	plain := newW17bTenantRig()
	framed := newW17bTenantRig()

	plainW := w17bPost(plain.router, w17bTenantPath, envelope("TenantStateChanged", eventID, tenantID, w17bEventTime, payload))
	framedW := w17bPost(framed.router, w17bTenantPath, envelopeWithSchemaID("TenantStateChanged", eventID, tenantID, w17bEventTime, uuid.NewString(), glueFrame(0x03, 0x00, raw)))

	require.Equal(t, http.StatusOK, plainW.Code, plainW.Body.String())
	require.Equal(t, http.StatusOK, framedW.Code, framedW.Body.String())
	require.Len(t, framed.lifecycle.inputs, 1)
	got := framed.lifecycle.inputs[0]
	assert.Equal(t, tenantID, got.TenantID)
	assert.Equal(t, "suspended", got.Status)
	assert.Equal(t, "active", got.PreviousStatus)
	assert.Equal(t, "pro", got.Plan)
	assert.Equal(t, "basic", got.PreviousPlan)
	assert.Equal(t, "non_payment", got.Cause)
	assert.True(t, w17bEventTime.Equal(got.ChangedAt))
	assert.Equal(t, plain.lifecycle.inputs, framed.lifecycle.inputs, "a framed payload is applied exactly like the plain one")
	assert.Equal(t, []w17bInboxCall{{eventID, w17bMembershipConsumer}}, framed.inbox.marked)
}

// A failing inbox read stops the event: it answers 503, applies nothing, and an error names the event.
func TestTenantEvents_TenantStateChanged_InboxReadFails_Answers503AndAppliesNothing(t *testing.T) {
	rig := newW17bTenantRig()
	rig.inbox.isErr = errW17bInboxRead
	tenantID, eventID := uuid.New(), uuid.New()

	w := w17bPost(rig.router, w17bTenantPath, envelope("TenantStateChanged", eventID, tenantID, w17bEventTime, w17bTenantStatePayload(tenantID, "suspended", w17bEventTime)))

	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	assert.Empty(t, rig.lifecycle.inputs, "the event waits for a redelivery")
	assert.Equal(t, []w17bInboxCall{{eventID, w17bMembershipConsumer}}, rig.inbox.checked)
	assert.Empty(t, rig.inbox.marked)
	line, ok := rig.log.errorWith(eventID, "claiming or recording")
	require.True(t, ok, "an error names the event whose inbox read failed: %v", rig.log.errs)
	assert.Equal(t, "TenantStateChanged", line.Fields["event_type"])
	assert.Equal(t, errW17bInboxRead.Error(), line.Fields["error"], "the cause goes to the log")
}

// A failing inbox write after the change was applied answers 503 with an error, so the delivery is retried.
// The retry is not deduplicated by the inbox, but the recency value committed by the first delivery skips it.
func TestTenantEvents_TenantStateChanged_InboxWriteFails_Answers503AndARedeliveryIsSkippedByRecency(t *testing.T) {
	rig := newW17bTenantRig()
	rig.inbox.markErr = errW17bInboxWrite
	tenantID, eventID := uuid.New(), uuid.New()
	body := envelope("TenantStateChanged", eventID, tenantID, w17bEventTime, w17bTenantStatePayload(tenantID, "suspended", w17bEventTime))

	first := w17bPost(rig.router, w17bTenantPath, body)
	second := w17bPost(rig.router, w17bTenantPath, body)

	require.Equal(t, http.StatusServiceUnavailable, first.Code, first.Body.String())
	require.Equal(t, http.StatusServiceUnavailable, second.Code, second.Body.String())
	line, ok := rig.log.errorWith(eventID, "claiming or recording")
	require.True(t, ok, "an error names the event whose inbox write failed: %v", rig.log.errs)
	assert.Equal(t, errW17bInboxWrite.Error(), line.Fields["error"])
	want := []w17bInboxCall{{eventID, w17bMembershipConsumer}, {eventID, w17bMembershipConsumer}}
	assert.Equal(t, want, rig.inbox.checked, "the inbox did not hold the event, so both deliveries were looked up")
	assert.Equal(t, want, rig.inbox.marked)
	require.Len(t, rig.lifecycle.inputs, 1, "the second delivery is skipped by the recency value, not applied twice")
	assert.Equal(t, tenantID, rig.lifecycle.inputs[0].TenantID)
	assert.Equal(t, []string{"ShouldApply", "Commit", "ShouldApply"}, rig.recency.methods())
}

// The offboarding sweep runs once, for the envelope tenant, under that tenant's row-level-security setting.
// That another tenant's instances stay untouched is shown against the database in
// TestTenantOffboarding_OneTenant_LeavesAnotherTenantsRunningInstancesRunning.
func TestTenantEvents_OffboardingOneTenant_SweepRunsUnderTheEnvelopeTenantsSetting(t *testing.T) {
	cases := []struct {
		name string
		body func(eventID, tenantID uuid.UUID) map[string]any
	}{
		{"TenantStateChanged offboarded", func(eventID, tenantID uuid.UUID) map[string]any {
			return envelope("TenantStateChanged", eventID, tenantID, w17bEventTime, w17bTenantStatePayload(tenantID, "offboarded", w17bEventTime))
		}},
		{"TenantMembershipsPurged", func(eventID, tenantID uuid.UUID) map[string]any {
			return envelope("TenantMembershipsPurged", eventID, tenantID, w17bEventTime, map[string]any{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW17bTenantRig()
			tenantA := uuid.New()

			w := w17bPost(rig.router, w17bTenantPath, tc.body(uuid.New(), tenantA))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, rig.lifecycle.inputs, 1, "one sweep, for one tenant")
			assert.Equal(t, tenantA, rig.lifecycle.inputs[0].TenantID)
			assert.Equal(t, "offboarded", rig.lifecycle.inputs[0].Status)
			assert.Equal(t, []string{tenantA.String()}, rig.lifecycle.ctxTenants)
		})
	}
}

// A payload tenant_id that is missing, empty, null or not a UUID is refused before anything is read or applied.
func TestTenantEvents_TenantStateChanged_PayloadTenantIDMissingOrMalformed_Returns400(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(payload map[string]any)
	}{
		{"missing", func(p map[string]any) { delete(p, "tenant_id") }},
		{"empty", func(p map[string]any) { p["tenant_id"] = "" }},
		{"null", func(p map[string]any) { p["tenant_id"] = nil }},
		{"not a UUID", func(p map[string]any) { p["tenant_id"] = "not-a-uuid" }},
		{"too short", func(p map[string]any) { p["tenant_id"] = "1111-1111" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW17bTenantRig()
			tenantID := uuid.New()
			payload := w17bTenantStatePayload(tenantID, "suspended", w17bEventTime)
			tc.mutate(payload)

			w := w17bPost(rig.router, w17bTenantPath, envelope("TenantStateChanged", uuid.New(), tenantID, w17bEventTime, payload))

			p := w17bDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "invalid tenant_id in payload", p.Detail)
			assert.Empty(t, rig.lifecycle.inputs)
			assert.Empty(t, rig.inbox.checked, "a refused event is not looked up in the inbox")
			assert.Empty(t, rig.inbox.marked)
			assert.Empty(t, rig.recency.calls)
		})
	}
}

// A changed_at that is missing, empty, a bare date or text is refused; fractional seconds and a zone
// offset are valid RFC 3339 and are applied with the exact instant.
func TestTenantEvents_TenantStateChanged_ChangedAt(t *testing.T) {
	refused := []struct {
		name   string
		mutate func(payload map[string]any)
	}{
		{"missing", func(p map[string]any) { delete(p, "changed_at") }},
		{"empty", func(p map[string]any) { p["changed_at"] = "" }},
		{"date only", func(p map[string]any) { p["changed_at"] = "2026-11-18" }},
		{"not a time", func(p map[string]any) { p["changed_at"] = "yesterday" }},
	}
	for _, tc := range refused {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			rig := newW17bTenantRig()
			tenantID := uuid.New()
			payload := w17bTenantStatePayload(tenantID, "suspended", w17bEventTime)
			tc.mutate(payload)

			w := w17bPost(rig.router, w17bTenantPath, envelope("TenantStateChanged", uuid.New(), tenantID, w17bEventTime, payload))

			p := w17bDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "invalid changed_at in payload", p.Detail)
			assert.Empty(t, rig.lifecycle.inputs)
			assert.Empty(t, rig.inbox.checked)
			assert.Empty(t, rig.inbox.marked)
		})
	}

	accepted := []struct {
		name string
		raw  string
		want time.Time
	}{
		{"fractional seconds", "2026-11-18T10:00:00.123456789Z", time.Date(2026, 11, 18, 10, 0, 0, 123456789, time.UTC)},
		{"zone offset", "2026-11-18T12:00:00+02:00", time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)},
	}
	for _, tc := range accepted {
		t.Run("accepted: "+tc.name, func(t *testing.T) {
			rig := newW17bTenantRig()
			tenantID := uuid.New()
			payload := w17bTenantStatePayload(tenantID, "suspended", w17bEventTime)
			payload["changed_at"] = tc.raw

			w := w17bPost(rig.router, w17bTenantPath, envelope("TenantStateChanged", uuid.New(), tenantID, w17bEventTime, payload))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, rig.lifecycle.inputs, 1)
			assert.True(t, tc.want.Equal(rig.lifecycle.inputs[0].ChangedAt), "got %s", rig.lifecycle.inputs[0].ChangedAt)
		})
	}
}
