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

func TestWorkflowClient_WithoutDelegationID_OnlyDelegationTaggedAssignmentsAreActedOn(t *testing.T) {
	seedMixed := func(h *csaClientHarness, tenantID, delegate uuid.UUID) (tagged, untagged, manual csaHeld) {
		tagged = h.seed(tenantID, delegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning)
		untagged = h.seed(tenantID, delegate, "", domain.TaskStatusReady, domain.InstanceStatusRunning)
		manual = h.seed(tenantID, delegate, "manual-assign", domain.TaskStatusReady, domain.InstanceStatusRunning)
		return tagged, untagged, manual
	}

	t.Run("cancel leaves the delegate's own tasks alone", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		tagged, untagged, manual := seedMixed(h, tenantID, delegate)

		count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, []csaIDCall{{tenantID, tagged.assignment.ID}}, h.assignments.vacated)
		assert.False(t, tagged.assignment.IsActive)
		assert.True(t, untagged.assignment.IsActive, "a task no delegation created is not cancelled")
		assert.True(t, manual.assignment.IsActive, "a task no delegation created is not cancelled")
	})

	t.Run("reassign leaves the delegate's own tasks alone", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		tagged, untagged, manual := seedMixed(h, tenantID, oldDelegate)

		count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, []csaIDCall{{tenantID, tagged.assignment.ID}}, h.assignments.vacated)
		assert.Equal(t, []csaCreateCall{{tenantID, tagged.task.ID, newDelegate}}, h.assignments.created)
		assert.True(t, untagged.assignment.IsActive)
		assert.True(t, manual.assignment.IsActive)
	})
}
