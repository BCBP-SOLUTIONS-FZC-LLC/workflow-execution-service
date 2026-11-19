package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceCancel_AWorkflowThatNoLongerExistsEndsTheInstanceRowLikeTerminateDoes(t *testing.T) {
	svc, instances, _, _, outbox, temporal, _, _ := newInstanceServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{
		ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 4,
		TemporalWorkflowID: "wf-" + instanceID.String(),
	}
	temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error {
		return fmt.Errorf("signal workflow: %w", port.ErrWorkflowNotFound)
	}

	err := svc.Cancel(context.Background(), tenantID, instanceID, uuid.New(), "cleanup", 4)

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[instanceID].Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, outbox.enqueued[0].Type)
}

func TestInstanceCancel_AnyOtherSignalFailureStillFailsTheCancelAndLeavesTheRow(t *testing.T) {
	svc, instances, _, _, outbox, temporal, _, _ := newInstanceServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{
		ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 4,
		TemporalWorkflowID: "wf-" + instanceID.String(),
	}
	temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

	err := svc.Cancel(context.Background(), tenantID, instanceID, uuid.New(), "cleanup", 4)

	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, domain.InstanceStatusRunning, instances.byID[instanceID].Status)
	assert.Empty(t, outbox.enqueued)
}
