package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestUserTaskPauser_SignalsEveryInstanceThatCanTakeAPause(t *testing.T) {
	cases := []struct {
		status domain.InstanceStatus
		signal bool
	}{
		{domain.InstanceStatusRunning, true},
		{domain.InstanceStatusPaused, true},
		{domain.InstanceStatusDegraded, true},
		{domain.InstanceStatusCompleted, false},
		{domain.InstanceStatusTerminated, false},
		{domain.InstanceStatusFailed, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			instances := newFakeInstanceRepo()
			tasks := newFakeTaskRepo()
			assignments := newFakeAssignmentRepo()
			temporal := &fakeTemporalClient{}
			tenantID, userID := uuid.New(), uuid.New()
			instanceID := uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: tc.status, TemporalWorkflowID: "tenant:biz", RecordVersion: 3}
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
			tasks.byID[task.ID] = task
			assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})

			pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
			require.NoError(t, pauser.PauseUserTasks(context.Background(), tenantID, userID, nil))

			if !tc.signal {
				assert.Empty(t, temporal.signals)
				return
			}
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)
			assert.Equal(t, instanceID, temporal.signals[0].InstanceID)
		})
	}
}
