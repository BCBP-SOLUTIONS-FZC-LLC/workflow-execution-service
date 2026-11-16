package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// A claim is a task-changing action like a completion: it is accepted while the
// instance is RUNNING or DEGRADED and refused with an invalid-instance-state
// answer in any other status, leaving the task without a lead.
func TestTaskClaim_InstanceStatus_OnlyRunningAndDegradedAreAccepted(t *testing.T) {
	cases := []struct {
		name    string
		status  domain.InstanceStatus
		wantErr error
	}{
		{"running", domain.InstanceStatusRunning, nil},
		{"degraded", domain.InstanceStatusDegraded, nil},
		{"paused", domain.InstanceStatusPaused, port.ErrInvalidInstanceState},
		{"completed", domain.InstanceStatusCompleted, port.ErrInvalidInstanceState},
		{"terminated", domain.InstanceStatusTerminated, port.ErrInvalidInstanceState},
		{"failed", domain.InstanceStatusFailed, port.ErrInvalidInstanceState},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := csdNewTaskHarness(t, "all", csdReviewStage())
			h.instances.byID[h.instanceID].Status = tc.status

			_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.Equal(t, h.callerID, h.assignments.leadOf(h.task.ID))
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
			h.requireTaskUntouched(t)
		})
	}
}
