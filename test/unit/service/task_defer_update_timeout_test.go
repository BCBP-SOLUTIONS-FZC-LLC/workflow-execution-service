package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// When the workflow's stage-defer update does not answer before the caller's
// deadline, Defer returns an error that still carries the deadline (so the
// request is answered as a timeout) and is not read as a refusal of the defer.
func TestTaskService_Defer_UpdateNotAnsweredBeforeTheDeadlineKeepsTheDeadlineError(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
	done := newReadyTask(tenantID, instanceID)
	done.Status = domain.TaskStatusCompleted
	tasks.byID[done.ID] = done
	task := newReadyTask(tenantID, instanceID)
	tasks.byID[task.ID] = task
	userID := uuid.New()
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
	temporal.updateFunc = func(context.Context, string, uuid.UUID, string, any) error {
		return context.DeadlineExceeded
	}

	_, err := svc.Defer(context.Background(), tenantID, task.ID, userID, "needs rework", task.RecordVersion)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.NotErrorIs(t, err, port.ErrInvalidTaskState)
	require.Len(t, temporal.updates, 1)
	assert.Equal(t, "tenant:biz", temporal.updates[0].TemporalWorkflowID)
	assert.Equal(t, instanceID, temporal.updates[0].InstanceID)
	assert.Equal(t, "stage-defer", temporal.updates[0].SignalName)
}
