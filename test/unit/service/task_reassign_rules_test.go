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

func TestTaskReassign_InstanceStatus_OnlyRunningOrDegradedIsAccepted(t *testing.T) {
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
			h := newTaHarness(t, "single")
			h.instances.byID[h.instanceID].Status = tc.status
			newUser := uuid.New()

			_, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), newUser, csdTaskVersion)

			if !tc.refused {
				require.NoError(t, err)
				require.Len(t, h.temporalLog.signals, 1)
				return
			}
			require.ErrorIs(t, err, port.ErrInvalidInstanceState)
			assert.Empty(t, h.temporalLog.signals, "the workflow would drop the signal, so none is sent")
			assert.Empty(t, h.elig.calls, "the instance state is refused before eligibility is asked")
		})
	}
}

func TestTaskReassign_TaskWithNoActiveAssignee_AcceptsAndSignalsNoPreviousUser(t *testing.T) {
	cases := []struct {
		name  string
		mode  string
		setup func(h *taHarness)
	}{
		{"single mode, every assignment vacated", "single", func(h *taHarness) { h.vacateAll() }},
		{"all mode, nobody claimed yet", "all", func(*taHarness) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTaHarness(t, tc.mode)
			tc.setup(h)
			newUser, admin := uuid.New(), uuid.New()

			got, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, admin, newUser, csdTaskVersion)

			require.NoError(t, err)
			assert.Equal(t, h.task.ID, got.ID)
			require.Len(t, h.temporalLog.signals, 1)
			sig := h.temporalLog.signals[0]
			assert.Equal(t, port.SignalInstanceReassign, sig.Name)
			assert.Equal(t, h.instanceID, sig.InstanceID)
			wire := taDecodeReassign(t, sig)
			assert.Empty(t, wire.OldUserID, "no previous user is carried, not the all-zero id")
			assert.Equal(t, newUser.String(), wire.NewUserID)
			assert.Equal(t, admin.String(), wire.AdminUserID)
			assert.Equal(t, h.task.ID.String(), wire.TaskID)
			assert.EqualValues(t, csdTaskVersion, wire.RecordVersion)
		})
	}
}

func TestTaskReassign_AllModeWithALead_SignalsTheLeadAsPreviousUser(t *testing.T) {
	h := newTaHarness(t, "all")
	h.other.IsLead = true
	newUser := uuid.New()

	_, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), newUser, csdTaskVersion)

	require.NoError(t, err)
	require.Len(t, h.temporalLog.signals, 1)
	assert.Equal(t, h.otherID.String(), taDecodeReassign(t, h.temporalLog.signals[0]).OldUserID)
}
