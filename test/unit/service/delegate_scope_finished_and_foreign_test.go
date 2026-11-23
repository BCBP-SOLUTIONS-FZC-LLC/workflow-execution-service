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

func TestWorkflowClient_ReassignDelegate_LeavesFinishedInstancesAndClosedTasksWithTheOldDelegate(t *testing.T) {
	h := csaNewClientHarness()
	tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
	reason := "delegation:" + uuid.NewString()
	open := h.seed(tenantID, oldDelegate, reason, domain.TaskStatusReady, domain.InstanceStatusRunning)
	var left []csaHeld
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed} {
		left = append(left, h.seed(tenantID, oldDelegate, reason, domain.TaskStatusReady, status))
	}
	left = append(left, h.seed(tenantID, oldDelegate, reason, domain.TaskStatusCompleted, domain.InstanceStatusRunning))

	count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.False(t, open.assignment.IsActive, "the open task of a live instance moves")
	for _, held := range left {
		assert.True(t, held.assignment.IsActive, "a %s task on a %s instance stays with the old delegate", held.task.Status, held.instance.Status)
	}
}

func TestWorkflowClient_CancelByDelegate_AnotherTenantsAssignmentsAreLeftAloneAndTheAnswerIsAZeroCount(t *testing.T) {
	h := csaNewClientHarness()
	ownerTenant, otherTenant, delegate := uuid.New(), uuid.New(), uuid.New()
	held := h.seed(ownerTenant, delegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning)

	count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: otherTenant, DelegateUserID: delegate})

	require.NoError(t, err, "the route never answers TENANT_MISMATCH: the tenant in the call scopes the read")
	assert.Zero(t, count)
	assert.True(t, held.assignment.IsActive)
}
