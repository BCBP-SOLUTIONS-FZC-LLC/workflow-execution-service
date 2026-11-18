package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestTenantLifecycleReconciler_Apply_UnrecognisedStatus_KeepsNothingAndWarns(t *testing.T) {
	changedAt := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		status string
		// known marks the control: a recognised status with the same inputs does act.
		known bool
	}{
		{name: "a status outside the known set", status: "frozen"},
		{name: "the known set is case sensitive", status: "Suspended"},
		{name: "an empty status", status: ""},
		{name: "control: a recognised status acts on the same inputs", status: domain.TenantStatusSuspended, known: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, _, outbox, temporal := newTenantLifecycleHarness()
			log := &fakeLogger{}
			states := &fakeTenantStates{}
			retention := &fakeRetention{}
			svc.Log, svc.States, svc.Retention = log, states, retention
			tenantID := uuid.New()
			running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
			instances.byID[running.ID] = running

			err := svc.Apply(context.Background(), port.TenantLifecycleInput{
				TenantID: tenantID, Status: tc.status, PreviousStatus: domain.TenantStatusActive,
				ChangedAt: changedAt,
			})

			require.NoError(t, err)
			if tc.known {
				assert.Equal(t, []recordedState{{tenantID, tc.status, changedAt}}, states.recorded)
				assert.Equal(t, []recordedReturn{{tenantID, changedAt}}, retention.returned)
				require.Len(t, temporal.signals, 1)
				assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)
				assert.Empty(t, log.warnCalls)
				return
			}
			assert.Empty(t, states.recorded, "the status is not kept")
			assert.Empty(t, retention.returned)
			assert.Empty(t, retention.recorded)
			assert.Empty(t, temporal.signals, "no pause or resume is sent")
			assert.Empty(t, temporal.terminate, "nothing is terminated")
			assert.Empty(t, outbox.enqueued)
			assert.Equal(t, domain.InstanceStatusRunning, running.Status)
			require.Len(t, log.warnCalls, 1, "one warning is logged")
			assert.Equal(t, tenantID, log.warnFields[0]["tenant_id"], "and it names the tenant")
		})
	}
}
