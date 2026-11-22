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

func TestWorkflowClient_CancelByDelegate_AssignmentsOnClosedTasksAreLeftAlone(t *testing.T) {
	h := csaNewClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	reason := "delegation:" + uuid.NewString()
	var open, closed []csaHeld
	for _, status := range []domain.TaskStatus{domain.TaskStatusReady, domain.TaskStatusInProgress, domain.TaskStatusDeferred} {
		open = append(open, h.seed(tenantID, delegate, reason, status, domain.InstanceStatusRunning))
	}
	for _, status := range []domain.TaskStatus{domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusSuperseded} {
		closed = append(closed, h.seed(tenantID, delegate, reason, status, domain.InstanceStatusRunning))
	}

	count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})

	require.NoError(t, err)
	assert.Equal(t, len(open), count)
	for _, held := range open {
		assert.False(t, held.assignment.IsActive, "an assignment on a %s task is vacated", held.task.Status)
	}
	for _, held := range closed {
		assert.True(t, held.assignment.IsActive, "an assignment on a %s task is left alone", held.task.Status)
	}
}
