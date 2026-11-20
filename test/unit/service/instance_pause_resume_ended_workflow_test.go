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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestInstancePauseAndResume_AWorkflowThatNoLongerExistsEndsTheInstanceRow(t *testing.T) {
	cases := []struct {
		name   string
		status domain.InstanceStatus
		act    func(*service.InstanceService, uuid.UUID, uuid.UUID) error
	}{
		{"pause", domain.InstanceStatusRunning, func(s *service.InstanceService, tenantID, id uuid.UUID) error {
			return s.Pause(context.Background(), tenantID, id, uuid.New(), "cleanup", 4)
		}},
		{"resume", domain.InstanceStatusPaused, func(s *service.InstanceService, tenantID, id uuid.UUID) error {
			return s.Resume(context.Background(), tenantID, id, uuid.New(), 4)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, outbox, temporal, _, _ := newInstanceServiceHarness()
			tenantID, instanceID := uuid.New(), uuid.New()
			instances.byID[instanceID] = &domain.Instance{
				ID: instanceID, TenantID: tenantID, Status: tc.status, RecordVersion: 4,
				TemporalWorkflowID: "wf-" + instanceID.String(),
			}
			temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error {
				return fmt.Errorf("signal workflow: %w", port.ErrWorkflowNotFound)
			}

			err := tc.act(svc, tenantID, instanceID)

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[instanceID].Status)
			require.Len(t, outbox.enqueued, 1)
			assert.Equal(t, domain.EventWorkflowInstanceTerminated, outbox.enqueued[0].Type)
		})
	}
}

func TestInstancePauseAndResume_AnyOtherSignalFailureStillFailsAndLeavesTheRow(t *testing.T) {
	svc, instances, _, _, outbox, temporal, _, _ := newInstanceServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{
		ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 4,
		TemporalWorkflowID: "wf-" + instanceID.String(),
	}
	temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

	err := svc.Pause(context.Background(), tenantID, instanceID, uuid.New(), "cleanup", 4)

	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, domain.InstanceStatusRunning, instances.byID[instanceID].Status)
	assert.Empty(t, outbox.enqueued)
}
