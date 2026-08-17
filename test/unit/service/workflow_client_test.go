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

func newWorkflowClientHarness() (*service.WorkflowClient, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeTemporalClient) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	temporal := &fakeTemporalClient{}
	return &service.WorkflowClient{Instances: instances, Tasks: tasks, Assignments: assignments, Temporal: temporal}, instances, tasks, assignments, temporal
}

func TestWorkflowClient_ReassignDelegate(t *testing.T) {
	t.Run("reassigns every active assignment, no delegation filter", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, assignments.byID[a.ID].IsActive, "the old assignment must be vacated")
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, temporal.signals[0].SignalName)

		var foundNew bool
		for _, na := range assignments.byID {
			if na.UserID == newDelegate && na.TaskID == task.ID {
				foundNew = true
			}
		}
		assert.True(t, foundNew, "a new assignment for the new delegate must be created")
	})

	t.Run("delegation_id filters to only that delegation's tagged rows", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		delegationID := uuid.New()

		taggedTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
		tasks.byID[taggedTask.ID] = taggedTask
		tagged := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taggedTask.ID, UserID: oldDelegate, IsActive: true, Reason: "delegation:" + delegationID.String()}
		assignments.byID[tagged.ID] = tagged

		untaggedTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
		tasks.byID[untaggedTask.ID] = untaggedTask
		untagged := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: untaggedTask.ID, UserID: oldDelegate, IsActive: true, Reason: "manual-assign"}
		assignments.byID[untagged.ID] = untagged

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
			TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate, DelegationID: &delegationID,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.False(t, assignments.byID[tagged.ID].IsActive)
		assert.True(t, assignments.byID[untagged.ID].IsActive, "an untagged assignment must survive an explicit delegation_id filter")
	})

	t.Run("zero matching assignments is a valid, non-error outcome", func(t *testing.T) {
		svc, _, _, _, _ := newWorkflowClientHarness()
		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: uuid.New(), OldDelegateID: uuid.New(), NewDelegateID: uuid.New()})
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		assignments.listActiveByUserErr = assert.AnError
		_, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: uuid.New(), OldDelegateID: uuid.New(), NewDelegateID: uuid.New()})
		assert.Error(t, err)
	})

	t.Run("an unreadable task is logged and skipped, not counted", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
	})

	t.Run("a vacate failure is logged and skipped, not counted", func(t *testing.T) {
		svc, instances, tasks, assignments, _ := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		assignments.vacateErr = assert.AnError
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("a create failure is logged and skipped, not counted", func(t *testing.T) {
		svc, instances, tasks, assignments, _ := newWorkflowClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a
		assignments.createErr = assert.AnError

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.False(t, assignments.byID[a.ID].IsActive, "the old assignment is already vacated even though the replacement failed to create")
	})

	t.Run("a signal failure is logged but still counted, DB state already updated", func(t *testing.T) {
		svc, instances, tasks, assignments, temporal := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		instanceID := uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, RecordVersion: 1}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldDelegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.NotEmpty(t, log.warnCalls)
	})
}

func TestWorkflowClient_CancelByDelegate(t *testing.T) {
	svc, _, tasks, assignments, _ := newWorkflowClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
	tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegate, IsActive: true}
	assignments.byID[a.ID] = a

	count, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.False(t, assignments.byID[a.ID].IsActive)
}

func TestWorkflowClient_CancelByDelegate_Errors(t *testing.T) {
	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		assignments.listActiveByUserErr = assert.AnError
		_, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: uuid.New(), DelegateUserID: uuid.New()})
		assert.Error(t, err)
	})

	t.Run("a vacate failure is logged and skipped, not counted", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		log := &fakeLogger{}
		svc.Log = log
		assignments.vacateErr = assert.AnError
		tenantID, delegate := uuid.New(), uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
		tasks.byID[task.ID] = task
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegate, IsActive: true}
		assignments.byID[a.ID] = a

		count, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.NotEmpty(t, log.warnCalls)
	})
}

func TestWorkflowClient_DelegateImpact_Errors(t *testing.T) {
	t.Run("ListActiveByUser error propagates", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		assignments.listActiveByUserErr = assert.AnError
		_, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: uuid.New(), DelegateUserID: uuid.New()})
		assert.Error(t, err)
	})

	t.Run("an unreadable task is skipped from the preview", func(t *testing.T) {
		svc, _, _, assignments, _ := newWorkflowClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: uuid.New(), UserID: delegate, IsActive: true}
		assignments.byID[a.ID] = a

		result, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate})
		require.NoError(t, err)
		assert.Empty(t, result.WorkflowIDs.Items)
	})

	t.Run("Page.Limit caps the previewed workflow IDs", func(t *testing.T) {
		svc, _, tasks, assignments, _ := newWorkflowClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		for i := 0; i < 3; i++ {
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: uuid.New()}
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegate, IsActive: true}
		}

		result, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 2}})
		require.NoError(t, err)
		assert.Len(t, result.WorkflowIDs.Items, 2)
	})
}

func TestWorkflowClient_DelegateImpact(t *testing.T) {
	svc, _, tasks, assignments, _ := newWorkflowClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	instanceID := uuid.New()

	// Two tasks on the same instance must dedupe to one workflow ID.
	taskA := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
	taskB := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID}
	tasks.byID[taskA.ID] = taskA
	tasks.byID[taskB.ID] = taskB
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskA.ID, UserID: delegate, IsActive: true}
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskB.ID, UserID: delegate, IsActive: true}

	result, err := svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}})
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReassignedCount)
	require.Len(t, result.WorkflowIDs.Items, 1, "two tasks on the same instance must dedupe to one workflow id")
	assert.Equal(t, instanceID, result.WorkflowIDs.Items[0])
}
