package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
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
		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstancePause, temporal.signals[0].SignalName)
		assert.Equal(t, instanceID, temporal.signals[0].InstanceID)

		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"Initiator":"`+domain.InitiatorSafetyNet+`"`, "the signal must carry safety_net as its initiator, not fall back to admin")
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
		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)
		require.NoError(t, err, "a non-pausable instance is logged and skipped, not a batch failure")
		assert.Empty(t, temporal.signals)
	})

	t.Run("no active assignments is a valid, non-error outcome", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: newFakeAssignmentRepo(), Tasks: newFakeTaskRepo(), Temporal: &fakeTemporalClient{}}
		err := pauser.PauseUserTasks(context.Background(), uuid.New(), uuid.New(), nil)
		require.NoError(t, err)
	})

	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		assignments := newFakeAssignmentRepo()
		assignments.listActiveByUserErr = assert.AnError
		pauser := &service.UserTaskPauser{Instances: newFakeInstanceRepo(), Assignments: assignments, Tasks: newFakeTaskRepo(), Temporal: &fakeTemporalClient{}}
		err := pauser.PauseUserTasks(context.Background(), uuid.New(), uuid.New(), nil)
		assert.Error(t, err)
	})

	t.Run("an unreadable task is returned as an error and the other instances are still paused", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{}
		obstest.Init(t)
		before := obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "read_task"})
		tenantID, userID := uuid.New(), uuid.New()

		assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: userID, IsActive: true})
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:ok"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)
		require.Error(t, err, "a revoked user's task that cannot be read must not be acknowledged as paused")
		require.Len(t, temporal.signals, 1, "the readable instance is still paused")
		assert.Equal(t, before+1, obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "read_task"}))
	})

	t.Run("an unreadable instance is returned as an error", func(t *testing.T) {
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		log := &fakeLogger{}
		obstest.Init(t)
		before := obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "read_instance"})
		tenantID, userID := uuid.New(), uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
		tasks.byID[task.ID] = task
		assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
		pauser := &service.UserTaskPauser{Instances: newFakeInstanceRepo(), Assignments: assignments, Tasks: tasks, Temporal: &fakeTemporalClient{}, Log: log}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)
		require.Error(t, err)
		assert.NotEmpty(t, log.warnCalls)
		assert.Equal(t, before+1, obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "read_instance"}))
	})

	t.Run("a signal failure is returned and every instance is still attempted", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		var attempted []uuid.UUID
		temporal := &fakeTemporalClient{signalFunc: func(_ context.Context, _ string, id uuid.UUID, _ string, _ any) error {
			attempted = append(attempted, id)
			return assert.AnError
		}}
		log := &fakeLogger{}
		obstest.Init(t)
		before := obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "signal"})
		tenantID, userID := uuid.New(), uuid.New()
		for range 2 {
			instanceID := uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
			tasks.byID[task.ID] = task
			assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
		}
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Log: log}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
		assert.Len(t, attempted, 2, "one failing instance must not stop the others")
		assert.NotEmpty(t, log.warnCalls)
		assert.Equal(t, before+2, obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "signal"}))
	})

	t.Run("a workflow that is already gone is success, a real failure beside it still fails", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		gone, broken := uuid.New(), uuid.New()
		temporal := &fakeTemporalClient{signalFunc: func(_ context.Context, _ string, id uuid.UUID, _ string, _ any) error {
			if id == gone {
				return fmt.Errorf("signal workflow %q: %w: %w", "x", port.ErrWorkflowNotFound, serviceerror.NewNotFound("workflow execution already completed"))
			}
			return assert.AnError
		}}
		obstest.Init(t)
		failures := func() float64 {
			return obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": "signal"})
		}
		before := failures()
		tenantID, userID := uuid.New(), uuid.New()
		for _, instanceID := range []uuid.UUID{gone, broken} {
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz"}
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
			tasks.byID[task.ID] = task
			assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
		}
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}

		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)

		require.ErrorIs(t, err, assert.AnError, "the real failure still fails the batch")
		assert.NotErrorIs(t, err, port.ErrWorkflowNotFound, "the instance that is already gone is not part of the failure")
		assert.Equal(t, before+1, failures(), "only the real failure is counted")

		delete(instances.byID, broken)
		instances.byID[broken] = &domain.Instance{ID: broken, TenantID: tenantID, Status: domain.InstanceStatusPaused}
		require.NoError(t, pauser.PauseUserTasks(context.Background(), tenantID, userID, nil), "with only the gone instance left the pause succeeds")
		assert.Equal(t, before+1, failures())
	})

	t.Run("an instance already paused is skipped without an error", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		temporal := &fakeTemporalClient{}
		tenantID, userID := uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: domain.InstanceStatusPaused}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
		tasks.byID[task.ID] = task
		assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
		pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}

		require.NoError(t, pauser.PauseUserTasks(context.Background(), tenantID, userID, nil))
		assert.Empty(t, temporal.signals)
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
		err := pauser.PauseUserTasks(context.Background(), tenantID, userID, nil)
		require.NoError(t, err)
		assert.Len(t, temporal.signals, 1)
	})
}
