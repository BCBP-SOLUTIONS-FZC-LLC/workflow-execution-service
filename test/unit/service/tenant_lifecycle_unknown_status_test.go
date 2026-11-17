package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestTenantLifecycleReconciler_Apply_UnrecognisedStatus_ChangesNothing(t *testing.T) {
	for _, status := range []string{"Suspended", "OFFBOARDED", "frozen", ""} {
		t.Run(status, func(t *testing.T) {
			svc, instances, _, _, _, outbox, temporal := newTenantLifecycleHarness()
			tenantID := uuid.New()
			running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
			instances.byID[running.ID] = running

			err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: status, PreviousStatus: "active"})

			require.NoError(t, err)
			assert.Empty(t, temporal.signals, "no pause or resume is sent")
			assert.Empty(t, temporal.terminate, "nothing is terminated")
			assert.Empty(t, outbox.enqueued)
			assert.Equal(t, domain.InstanceStatusRunning, instances.byID[running.ID].Status)
		})
	}
}
