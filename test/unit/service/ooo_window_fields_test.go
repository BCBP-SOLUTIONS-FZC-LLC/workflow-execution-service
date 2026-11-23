package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestOOOAvailabilityReconciler_Apply_TheWindowFieldsNeitherStartNorEndAPause(t *testing.T) {
	now := time.Now().UTC()
	past, soon, later := now.Add(-time.Hour), now.Add(time.Hour), now.Add(48*time.Hour)

	cases := []struct {
		name       string
		status     string
		from, till *time.Time
		wantSignal string
	}{
		{"available with a future window pauses nothing", port.AvailabilityAvailable, &soon, &later, port.SignalInstanceResume},
		{"ooo pauses at once although the window starts later", port.AvailabilityOOO, &soon, &later, port.SignalInstancePause},
		{"ooo pauses although the window has already ended", port.AvailabilityOOO, &past, &past, port.SignalInstancePause},
		{"ooo with no window pauses", port.AvailabilityOOO, nil, nil, port.SignalInstancePause},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
			tenantID, userID, instanceID := uuid.New(), uuid.New(), uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

			err := svc.Apply(context.Background(), port.UserAvailabilityInput{
				TenantID: tenantID, UserID: userID, Status: tc.status, OOOFrom: tc.from, OOOUntil: tc.till, ChangedAt: now,
			})

			require.NoError(t, err)
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, tc.wantSignal, temporal.signals[0].SignalName)
		})
	}
}

func TestOOOAvailabilityReconciler_Apply_ALaterEventReplacesTheEarlierOne(t *testing.T) {
	svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
	tenantID, userID, instanceID := uuid.New(), uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
	tasks.byID[task.ID] = task
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
	start := time.Now().UTC()
	until := start.Add(72 * time.Hour)

	require.NoError(t, svc.Apply(context.Background(), port.UserAvailabilityInput{
		TenantID: tenantID, UserID: userID, Status: port.AvailabilityOOO, OOOFrom: &start, OOOUntil: &until, ChangedAt: start,
	}))
	require.NoError(t, svc.Apply(context.Background(), port.UserAvailabilityInput{
		TenantID: tenantID, UserID: userID, Status: port.AvailabilityAvailable, ChangedAt: start.Add(time.Minute),
	}))

	require.Len(t, temporal.signals, 2)
	assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)
	assert.Equal(t, port.SignalInstanceResume, temporal.signals[1].SignalName, "the user set available before the window ended, so the pause is lifted at once")
}
