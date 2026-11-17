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

func TestTaskService_Defer_IsAcceptedOnADegradedInstanceAndRefusedOnOtherNonRunningOnes(t *testing.T) {
	cases := []struct {
		status  domain.InstanceStatus
		refused bool
	}{
		{domain.InstanceStatusRunning, false},
		{domain.InstanceStatusDegraded, false},
		{domain.InstanceStatusPaused, true},
		{domain.InstanceStatusCompleted, true},
		{domain.InstanceStatusTerminated, true},
		{domain.InstanceStatusFailed, true},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
			tenantID, instanceID := uuid.New(), uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: tc.status, TemporalWorkflowID: "tenant:biz"}
			done := newReadyTask(tenantID, instanceID)
			done.Status = domain.TaskStatusCompleted
			tasks.byID[done.ID] = done
			task := newReadyTask(tenantID, instanceID)
			tasks.byID[task.ID] = task
			userID := uuid.New()
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

			_, err := svc.Defer(context.Background(), tenantID, task.ID, userID, "reason", task.RecordVersion)

			if tc.refused {
				require.ErrorIs(t, err, port.ErrInvalidInstanceState)
				assert.Empty(t, temporal.updates)
				return
			}
			require.NoError(t, err)
			require.Len(t, temporal.updates, 1)
			assert.Equal(t, "stage-defer", temporal.updates[0].SignalName)
			assert.Equal(t, instanceID, temporal.updates[0].InstanceID)
		})
	}
}
