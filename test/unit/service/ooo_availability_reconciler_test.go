package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func newOOOAvailabilityHarness() (*service.OOOAvailabilityReconciler, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeTemporalClient) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	temporal := &fakeTemporalClient{}
	svc := &service.OOOAvailabilityReconciler{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
	return svc, instances, tasks, assignments, temporal
}

func TestOOOAvailabilityReconciler_Apply(t *testing.T) {
	t.Run("status ooo with no delegate pauses every RUNNING instance", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
		tenantID, userID := uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "ooo"})
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)

		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"Initiator":"`+domain.InitiatorOOO+`"`, "the signal must carry ooo as its initiator, not fall back to admin")
	})

	t.Run("status ooo with an active delegate is a no-op", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
		tenantID, userID, delegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "ooo", DelegateUserID: &delegate})
		require.NoError(t, err)
		assert.Empty(t, temporal.signals, "an active delegation already routes tasks away — no pause is needed")
	})

	t.Run("status available resumes a PAUSED instance among active assignments", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
		tenantID, userID := uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusPaused, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "available"})
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceResume, temporal.signals[0].SignalName)

		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"Initiator":"`+domain.InitiatorOOO+`"`, "the signal must carry ooo as its initiator, not fall back to admin")
	})

	t.Run("status available skips a RUNNING instance", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
		tenantID, userID := uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "available"})
		require.NoError(t, err)
		assert.Empty(t, temporal.signals)
	})

	t.Run("unknown status is a no-op", func(t *testing.T) {
		svc, _, _, _, temporal := newOOOAvailabilityHarness()
		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: uuid.New(), UserID: uuid.New(), Status: "something-else"})
		require.NoError(t, err)
		assert.Empty(t, temporal.signals)
	})

	t.Run("an unreadable task is logged and skipped, not a batch failure", func(t *testing.T) {
		svc, _, _, assignments, temporal := newOOOAvailabilityHarness()
		tenantID, userID := uuid.New(), uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "ooo"})
		require.NoError(t, err, "an unreadable task falls back to the noop logger and must not panic or abort the batch")
		assert.Empty(t, temporal.signals)
	})

	t.Run("an unreadable instance is logged and skipped, not a batch failure", func(t *testing.T) {
		svc, _, tasks, assignments, temporal := newOOOAvailabilityHarness()
		log := &fakeLogger{}
		svc.Log = log
		tenantID, userID := uuid.New(), uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "ooo"})
		require.NoError(t, err)
		assert.Empty(t, temporal.signals)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("a signal failure is logged, not returned", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
		log := &fakeLogger{}
		svc.Log = log
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }
		tenantID, userID := uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: "ooo"})
		require.NoError(t, err)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newOOOAvailabilityHarness()
		assignments.listActiveByUserErr = assert.AnError
		err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: uuid.New(), UserID: uuid.New(), Status: "ooo"})
		assert.Error(t, err)
	})
}
