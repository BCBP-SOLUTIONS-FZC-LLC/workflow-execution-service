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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW17aTenantGuard = errors.New("w17a: tenant recency read failed: host=10.4.5.6")
	errW17aPurge       = errors.New("w17a: terminate instance 7c1 failed: password=hunter2")
)

func w17aTenantStateData(tenantID uuid.UUID, status string, at time.Time) map[string]any {
	return map[string]any{
		"tenant_id": tenantID.String(), "status": status, "previous_status": "active",
		"plan": "pro", "previous_plan": "pro", "changed_at": at.Format(time.RFC3339), "cause": "x",
	}
}

func TestTenantEvent_StateChangeGuardReadFails_Answers500AndAppliesNothing(t *testing.T) {
	obstest.Init(t)
	failed := map[string]string{"event_type": "TenantStateChanged", "result": "error"}
	cases := []struct {
		name   string
		status string
	}{
		{name: "a suspension", status: "suspended"},
		{name: "a reactivation", status: "active"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w17aNewRig()
			rig.recency.shouldApplyErr = errW17aTenantGuard
			tenantID, eventID := uuid.New(), uuid.New()
			at := time.Now().UTC().Truncate(time.Second)
			failedBefore := obstest.Sum(t, w17aIngestMetric, failed)

			w := rig.postEnvelope(w17aTenantRoute, envelope("TenantStateChanged", eventID, tenantID, at, w17aTenantStateData(tenantID, tc.status, at)))

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			p := w17aDecodeProblem(t, w)
			assert.Equal(t, http.StatusInternalServerError, p.Status)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, w17aInternalType, p.Type)
			assert.Equal(t, "Internal Server Error", p.Title)
			assert.Equal(t, "failed to process event", p.Detail)
			assert.NotContains(t, w.Body.String(), "10.4.5.6", "the error text is not echoed")
			assert.NotContains(t, w.Body.String(), "recency read")
			require.Len(t, rig.recency.locks, 1, "the lock was taken, so the failure is the read inside it")
			reads := rig.recency.shouldApply
			require.Len(t, reads, 1)
			assert.Equal(t, rig.recency.locks[0], reads[0].scopeKey, "the read is for the lock's own scope")
			assert.Contains(t, rig.recency.locks[0], tenantID.String())
			assert.True(t, at.Equal(reads[0].at))
			assert.Empty(t, rig.recency.unlessOlder, "only the plain read is made for a status that is not offboarded")
			assert.Empty(t, rig.tenant.calls, "the tenant's state is not touched")
			assert.Empty(t, rig.recency.commits)
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.lookups)
			assert.Empty(t, rig.inbox.marks, "the event is not marked processed")
			assert.Equal(t, failedBefore+1, obstest.Sum(t, w17aIngestMetric, failed))
		})
	}
}

func TestTenantEvent_OffboardedStateChange_DoesNotMakeThePlainGuardRead(t *testing.T) {
	rig := w17aNewRig()
	rig.recency.shouldApplyErr = errW17aTenantGuard
	tenantID, eventID := uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Second)

	w := rig.postEnvelope(w17aTenantRoute, envelope("TenantStateChanged", eventID, tenantID, at, w17aTenantStateData(tenantID, domain.TenantStatusOffboarded, at)))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Empty(t, rig.recency.shouldApply, "an offboarding is checked with the unless-older read instead")
	require.Len(t, rig.recency.unlessOlder, 1)
	assert.Equal(t, rig.recency.locks, []string{rig.recency.unlessOlder[0].scopeKey})
	assert.Equal(t, []w17aTenantCall{{tenantID: tenantID, status: domain.TenantStatusOffboarded, changedAt: at, gucTenant: tenantID.String()}}, rig.tenant.calls)
	assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.marks)
}

func TestTenantEvent_MembershipsPurgedReconcileFails_Answers500AndKeepsTheEventUnmarked(t *testing.T) {
	obstest.Init(t)
	failed := map[string]string{"event_type": "TenantMembershipsPurged", "result": "error"}
	succeeded := map[string]string{"event_type": "TenantMembershipsPurged", "result": "ok"}
	cases := []struct {
		name string
		err  error
	}{
		{name: "a failure while ending the tenant's instances", err: fmt.Errorf("offboard tenant: %w", errW17aPurge)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w17aNewRig()
			rig.tenant.err = tc.err
			tenantID, eventID := uuid.New(), uuid.New()
			at := time.Now().UTC().Truncate(time.Second)
			env := envelope("TenantMembershipsPurged", eventID, tenantID, at, map[string]any{})
			failedBefore, okBefore := obstest.Sum(t, w17aIngestMetric, failed), obstest.Sum(t, w17aIngestMetric, succeeded)

			w := rig.postEnvelope(w17aTenantRoute, env)

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			p := w17aDecodeProblem(t, w)
			assert.Equal(t, http.StatusInternalServerError, p.Status)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, w17aInternalType, p.Type)
			assert.Equal(t, "Internal Server Error", p.Title)
			assert.Equal(t, "failed to process event", p.Detail)
			assert.NotContains(t, w.Body.String(), "hunter2", "the error text is not echoed")
			assert.NotContains(t, w.Body.String(), "terminate instance")
			assert.Equal(t, []w17aTenantCall{{tenantID: tenantID, status: domain.TenantStatusOffboarded, changedAt: at, gucTenant: tenantID.String()}}, rig.tenant.calls)
			assert.Empty(t, rig.recency.commits, "no recency is recorded for a sweep that failed")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.lookups)
			assert.Empty(t, rig.inbox.marks, "the event is not marked processed, so it is delivered again")
			assert.Equal(t, failedBefore+1, obstest.Sum(t, w17aIngestMetric, failed))
			assert.Equal(t, okBefore, obstest.Sum(t, w17aIngestMetric, succeeded))

			rig.tenant.err = nil
			w = rig.postEnvelope(w17aTenantRoute, env)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Len(t, rig.tenant.calls, 2, "the redelivery runs the sweep again")
			assert.Equal(t, rig.tenant.calls[0], rig.tenant.calls[1], "for the same tenant and time")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.marks)
		})
	}
}
