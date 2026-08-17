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

func TestUserTaskPauser_PauseUserTasks(t *testing.T) {
	t.Run("pauses every running instance the user has an active assignment on", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{}
		tenantID, userID := uuid.New(), uuid.New()

		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz", RecordVersion: 2}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
		err := pauser.PauseUserTasks(context.Background(), tenantID, userID)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)
		assert.Equal(t, instanceID, temporal.signals[0].InstanceID)
	})

	t.Run("skips a non-RUNNING instance without failing the batch", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{}
		tenantID, userID := uuid.New(), uuid.New()

		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusPaused}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
		err := pauser.PauseUserTasks(context.Background(), tenantID, userID)
		require.NoError(t, err, "a non-pausable instance is logged and skipped, not a batch failure")
		assert.Empty(t, temporal.signals)
	})

	t.Run("no active assignments is a valid, non-error outcome", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: newFakeAssignmentRepo(), Tasks: newFakeTaskRepo(), Temporal: &fakeTemporalClient{}}
		err := pauser.PauseUserTasks(context.Background(), uuid.New(), uuid.New())
		require.NoError(t, err)
	})

	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		assignments := newFakeAssignmentRepo()
		assignments.listActiveByUserErr = assert.AnError
		pauser := &service.UserTaskPauser{Instances: newFakeInstanceRepo(), Assignments: assignments, Tasks: newFakeTaskRepo(), Temporal: &fakeTemporalClient{}}
		err := pauser.PauseUserTasks(context.Background(), uuid.New(), uuid.New())
		assert.Error(t, err)
	})

	t.Run("an unreadable task is logged and skipped, not a batch failure", func(t *testing.T) {
		assignments := newFakeAssignmentRepo()
		tenantID, userID := uuid.New(), uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: userID, IsActive: true}
		pauser := &service.UserTaskPauser{Instances: newFakeInstanceRepo(), Assignments: assignments, Tasks: newFakeTaskRepo(), Temporal: &fakeTemporalClient{}}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID)
		require.NoError(t, err)
	})

	t.Run("an unreadable instance is logged and skipped, not a batch failure", func(t *testing.T) {
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		log := &fakeLogger{}
		tenantID, userID := uuid.New(), uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		pauser := &service.UserTaskPauser{Instances: newFakeInstanceRepo(), Assignments: assignments, Tasks: tasks, Temporal: &fakeTemporalClient{}, Log: log}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID)
		require.NoError(t, err)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("a signal failure is logged, not returned", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{signalFunc: func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }}
		log := &fakeLogger{}
		tenantID, userID := uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Log: log}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID)
		require.NoError(t, err)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("two tasks on the same instance only signal once", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{}
		tenantID, userID := uuid.New(), uuid.New()

		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
		taskA := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		taskB := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[taskA.ID] = taskA
		tasks.byID[taskB.ID] = taskB
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskA.ID, UserID: userID, IsActive: true}
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskB.ID, UserID: userID, IsActive: true}

		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}
		err := pauser.PauseUserTasks(context.Background(), tenantID, userID)
		require.NoError(t, err)
		assert.Len(t, temporal.signals, 1)
	})
}
