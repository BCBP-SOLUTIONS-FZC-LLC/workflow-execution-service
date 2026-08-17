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

func newTenantLifecycleHarness() (*service.TenantLifecycleReconciler, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeActiveTaskQueueRepo, *fakeOutbox, *fakeTemporalClient) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	queues := newFakeActiveTaskQueueRepo()
	outbox := &fakeOutbox{}
	temporal := &fakeTemporalClient{}
	svc := &service.TenantLifecycleReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Queues: queues,
		Outbox: outbox, Transactor: fakeTransactor{}, Temporal: temporal, Validator: noopValidator{},
	}
	return svc, instances, tasks, assignments, queues, outbox, temporal
}

func TestTenantLifecycleReconciler_Apply_StatusTransitions(t *testing.T) {
	t.Run("offboarded terminates every non-terminal instance", func(t *testing.T) {
		svc, instances, tasks, assignments, _, outbox, temporal := newTenantLifecycleHarness()
		tenantID := uuid.New()

		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		paused := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusPaused, TemporalWorkflowID: "tenant:paused", RecordVersion: 1}
		completed := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusCompleted, TemporalWorkflowID: "tenant:done", RecordVersion: 1}
		instances.byID[running.ID] = running
		instances.byID[paused.ID] = paused
		instances.byID[completed.ID] = completed

		activeTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: running.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
		tasks.byID[activeTask.ID] = activeTask
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: activeTask.ID, UserID: uuid.New(), IsActive: true}
		assignments.byID[a.ID] = a

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"})
		require.NoError(t, err)

		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[running.ID].Status)
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[paused.ID].Status)
		assert.Equal(t, domain.InstanceStatusCompleted, instances.byID[completed.ID].Status, "an already-terminal instance must be left untouched")
		assert.Equal(t, domain.TaskStatusFailed, tasks.byID[activeTask.ID].Status)
		assert.False(t, assignments.byID[a.ID].IsActive)
		assert.Len(t, outbox.enqueued, 2)
		require.Len(t, temporal.terminate, 2)
	})

	t.Run("transition to active resumes tenant-state-paused instances", func(t *testing.T) {
		svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
		tenantID := uuid.New()
		paused := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusPaused, TemporalWorkflowID: "tenant:paused", RecordVersion: 1}
		instances.byID[paused.ID] = paused

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "active", PreviousStatus: "suspended"})
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceResume, temporal.signals[0].SignalName)
	})

	t.Run("transition to suspended pauses every RUNNING instance", func(t *testing.T) {
		svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
		tenantID := uuid.New()
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		instances.byID[running.ID] = running

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "suspended", PreviousStatus: "active"})
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)
	})

	t.Run("status unchanged is a no-op", func(t *testing.T) {
		svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
		tenantID := uuid.New()
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		instances.byID[running.ID] = running

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "active", PreviousStatus: "active"})
		require.NoError(t, err)
		assert.Empty(t, temporal.signals)
	})

	t.Run("a status-transition failure propagates from Apply", func(t *testing.T) {
		svc, instances, _, _, _, _, _ := newTenantLifecycleHarness()
		instances.listErr = assert.AnError
		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: uuid.New(), Status: "suspended", PreviousStatus: "active"})
		assert.Error(t, err)
	})

	t.Run("a signal failure during the tenant-wide sweep is logged, not returned", func(t *testing.T) {
		svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
		log := &fakeLogger{}
		svc.Log = log
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }
		tenantID := uuid.New()
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		instances.byID[running.ID] = running

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "suspended", PreviousStatus: "active"})
		require.NoError(t, err)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("pagination merges every page of a tenant-wide sweep", func(t *testing.T) {
		svc, instances, _, _, _, _, _ := newTenantLifecycleHarness()
		tenantID := uuid.New()
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		instances.byID[running.ID] = running
		instances.nextCursor = &port.Cursor{CreatedAt: running.CreatedAt, ID: running.ID}

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "suspended", PreviousStatus: "active"})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, instances.listByTenantCalls, 2, "a non-nil cursor on the first page must trigger a second ListByTenant call")
	})
}

func TestTenantLifecycleReconciler_Apply_PlanChange(t *testing.T) {
	t.Run("upgrade to enterprise registers the isolated queue", func(t *testing.T) {
		svc, _, _, _, queues, _, _ := newTenantLifecycleHarness()
		tenantID := uuid.New()

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "active", PreviousStatus: "active", Plan: "enterprise", PreviousPlan: "standard"})
		require.NoError(t, err)
		_, ok := queues.byName["wf-queue-"+tenantID.String()]
		assert.True(t, ok)
	})

	t.Run("downgrade deregisters the isolated queue when no active instances remain", func(t *testing.T) {
		svc, _, _, _, queues, _, _ := newTenantLifecycleHarness()
		tenantID := uuid.New()
		queueName := "wf-queue-" + tenantID.String()
		queues.byName[queueName] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: queueName}

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "active", PreviousStatus: "active", Plan: "standard", PreviousPlan: "enterprise"})
		require.NoError(t, err)
		_, ok := queues.byName[queueName]
		assert.False(t, ok)
	})

	t.Run("downgrade with active instances on the queue is deferred", func(t *testing.T) {
		svc, instances, _, _, queues, _, _ := newTenantLifecycleHarness()
		tenantID := uuid.New()
		queueName := "wf-queue-" + tenantID.String()
		queues.byName[queueName] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: queueName}
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TaskQueue: queueName}
		instances.byID[running.ID] = running

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "active", PreviousStatus: "active", Plan: "standard", PreviousPlan: "enterprise"})
		require.NoError(t, err)
		_, ok := queues.byName[queueName]
		assert.True(t, ok, "the isolated queue must stay registered while an instance is still running on it")
	})

	t.Run("plan unchanged is a no-op", func(t *testing.T) {
		svc, _, _, _, queues, _, _ := newTenantLifecycleHarness()
		tenantID := uuid.New()

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "active", PreviousStatus: "active", Plan: "standard", PreviousPlan: "standard"})
		require.NoError(t, err)
		assert.Empty(t, queues.byName)
	})

	t.Run("a Register failure propagates from Apply", func(t *testing.T) {
		svc, _, _, _, queues, _, _ := newTenantLifecycleHarness()
		queues.registerErr = assert.AnError
		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: uuid.New(), Status: "active", PreviousStatus: "active", Plan: "enterprise", PreviousPlan: "standard"})
		assert.Error(t, err)
	})

	t.Run("a CountActiveByTaskQueue failure propagates from Apply", func(t *testing.T) {
		svc, instances, _, _, _, _, _ := newTenantLifecycleHarness()
		instances.countErr = assert.AnError
		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: uuid.New(), Status: "active", PreviousStatus: "active", Plan: "standard", PreviousPlan: "enterprise"})
		assert.Error(t, err)
	})

	t.Run("a Deregister failure propagates from Apply", func(t *testing.T) {
		svc, _, _, _, queues, _, _ := newTenantLifecycleHarness()
		queues.deregisterErr = assert.AnError
		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: uuid.New(), Status: "active", PreviousStatus: "active", Plan: "standard", PreviousPlan: "enterprise"})
		assert.Error(t, err)
	})
}

func TestTenantLifecycleReconciler_Offboard_Termination(t *testing.T) {
	t.Run("an UpdateStatus failure during termination is logged, not returned", func(t *testing.T) {
		svc, instances, _, _, _, _, _ := newTenantLifecycleHarness()
		log := &fakeLogger{}
		svc.Log = log
		tenantID := uuid.New()
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		instances.byID[running.ID] = running
		instances.updateErr = assert.AnError

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"})
		require.NoError(t, err, "a per-instance termination failure during offboard is logged and skipped, not a batch failure")
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("a TerminateWorkflow failure is logged, not returned, DB state already committed", func(t *testing.T) {
		svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
		log := &fakeLogger{}
		svc.Log = log
		temporal.terminateFunc = func(context.Context, string, string) error { return assert.AnError }
		tenantID := uuid.New()
		running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
		instances.byID[running.ID] = running

		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"})
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[running.ID].Status)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("an allInstancesByStatus failure during offboard propagates from Apply", func(t *testing.T) {
		svc, instances, _, _, _, _, _ := newTenantLifecycleHarness()
		instances.listErr = assert.AnError
		err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: uuid.New(), Status: "offboarded", PreviousStatus: "active"})
		assert.Error(t, err)
	})
}
