package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func newTaskServiceHarness() (*service.TaskService, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeAssigneeOverrideRepo, *fakeTemporalClient) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	overrides := &fakeAssigneeOverrideRepo{}
	temporal := &fakeTemporalClient{}

	svc := &service.TaskService{
		Instances:   instances,
		Tasks:       tasks,
		Assignments: assignments,
		Overrides:   overrides,
		Temporal:    temporal,
	}
	return svc, instances, tasks, assignments, overrides, temporal
}

func TestTaskService_List(t *testing.T) {
	svc, _, tasks, _, _, _ := newTaskServiceHarness()
	tenantID := uuid.New()
	ready := &domain.Task{ID: uuid.New(), TenantID: tenantID, Status: domain.TaskStatusReady}
	completed := &domain.Task{ID: uuid.New(), TenantID: tenantID, Status: domain.TaskStatusCompleted}
	tasks.byID[ready.ID] = ready
	tasks.byID[completed.ID] = completed

	status := port.TaskStatusCompleted
	res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.TaskFilter{Status: &status}, port.Page{Limit: 10})
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	assert.Equal(t, completed.ID, res.Items[0].ID)

	t.Run("repo error propagates", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tasks.listErr = assert.AnError
		_, err := svc.List(context.Background(), uuid.New(), port.ReadScope{IsAdmin: true}, port.TaskFilter{}, port.Page{Limit: 10})
		assert.Error(t, err)
	})
}

func TestTaskService_Get(t *testing.T) {
	svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
	tenantID := uuid.New()
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, DepartmentID: uuid.New(), Status: domain.TaskStatusReady}
	tasks.byID[task.ID] = task

	t.Run("not found", func(t *testing.T) {
		_, _, err := svc.Get(context.Background(), tenantID, uuid.New(), port.ReadScope{IsAdmin: true})
		assert.ErrorIs(t, err, port.ErrTaskNotFound)
	})

	t.Run("admin bypasses scope", func(t *testing.T) {
		got, _, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{IsAdmin: true})
		require.NoError(t, err)
		assert.Equal(t, task.ID, got.ID)
	})

	t.Run("unrelated caller rejected", func(t *testing.T) {
		_, _, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{CallerUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	})

	t.Run("department match authorized", func(t *testing.T) {
		got, _, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{Departments: []string{task.DepartmentID.String()}})
		require.NoError(t, err)
		assert.Equal(t, task.ID, got.ID)
	})

	t.Run("active assignee authorized", func(t *testing.T) {
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		got, gotAssignments, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{CallerUserID: userID})
		require.NoError(t, err)
		assert.Equal(t, task.ID, got.ID)
		assert.Len(t, gotAssignments, 1)
	})

	t.Run("ListActiveByTask error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID}
		tasks.byID[task.ID] = task
		assignments.listActiveByTaskErr = assert.AnError

		_, _, err := svc.Get(context.Background(), tenantID, task.ID, port.ReadScope{IsAdmin: true})
		assert.Error(t, err)
	})
}

func TestTaskService_GetByNode(t *testing.T) {
	svc, _, tasks, _, _, _ := newTaskServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review"}
	tasks.byID[task.ID] = task

	got, err := svc.GetByNode(context.Background(), tenantID, instanceID, "finance/review")
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)

	_, err = svc.GetByNode(context.Background(), tenantID, instanceID, "finance/does-not-exist")
	assert.ErrorIs(t, err, port.ErrTaskNotFound)
}

func TestTaskService_ActiveByUser(t *testing.T) {
	svc, _, _, assignments, _, _ := newTaskServiceHarness()
	tenantID, userID := uuid.New(), uuid.New()
	taskID := uuid.New()
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: taskID, UserID: userID, IsActive: true}

	res, err := svc.ActiveByUser(context.Background(), tenantID, userID, port.Page{Limit: 10})
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	assert.Equal(t, taskID, res.Items[0].TaskID)

	t.Run("repo error propagates", func(t *testing.T) {
		svc, _, _, assignments, _, _ := newTaskServiceHarness()
		assignments.listActiveByUserPaginatedErr = assert.AnError
		_, err := svc.ActiveByUser(context.Background(), uuid.New(), uuid.New(), port.Page{Limit: 10})
		assert.Error(t, err)
	})
}

func newReadyTask(tenantID, instanceID uuid.UUID) *domain.Task {
	return &domain.Task{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/review",
		Status: domain.TaskStatusReady, AssigneeMode: "single", RecordVersion: 1,
	}
}

func TestTaskService_Claim(t *testing.T) {
	t.Run("success sets lead on a multi-assignee task", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		userID := uuid.New()
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		assignments.byID[a.ID] = a

		got, err := svc.Claim(context.Background(), tenantID, task.ID, userID, task.RecordVersion)
		require.NoError(t, err)
		assert.Equal(t, task.ID, got.ID)
		assert.True(t, assignments.byID[a.ID].IsLead)
	})

	t.Run("not applicable for single-assignee task", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		tasks.byID[task.ID] = task

		_, err := svc.Claim(context.Background(), tenantID, task.ID, uuid.New(), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrClaimNotApplicable)
	})

	t.Run("already claimed by another lead", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		lead := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true, IsLead: true}
		assignments.byID[lead.ID] = lead

		_, err := svc.Claim(context.Background(), tenantID, task.ID, uuid.New(), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrTaskAlreadyClaimed)
	})

	t.Run("caller is not an assignee", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task

		_, err := svc.Claim(context.Background(), tenantID, task.ID, uuid.New(), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrNotAssignee)
	})

	t.Run("record version conflict", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task

		_, err := svc.Claim(context.Background(), tenantID, task.ID, uuid.New(), 99)
		assert.ErrorIs(t, err, port.ErrRecordVersionConflict)
	})

	t.Run("connector-typed task is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		connectorType := "send-email"
		task.ConnectorType = &connectorType
		tasks.byID[task.ID] = task

		_, err := svc.Claim(context.Background(), tenantID, task.ID, uuid.New(), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrTaskNotHumanActionable)
	})

	t.Run("ListActiveByTask error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		assignments.listActiveByTaskErr = assert.AnError

		_, err := svc.Claim(context.Background(), tenantID, task.ID, uuid.New(), task.RecordVersion)
		assert.Error(t, err)
	})

	t.Run("SetLead error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		assignments.setLeadErr = assert.AnError

		_, err := svc.Claim(context.Background(), tenantID, task.ID, userID, task.RecordVersion)
		assert.Error(t, err)
	})
}

func TestTaskService_Complete(t *testing.T) {
	t.Run("success signals stage-transition with the right node parts", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		inst := &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		instances.byID[instanceID] = inst
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{"decision":"approve"}`), task.RecordVersion)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, "stage-transition", temporal.signals[0].SignalName)
		assert.Equal(t, "tenant:biz", temporal.signals[0].TemporalWorkflowID)
		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"DeptID":"finance"`)
		assert.Contains(t, string(b), `"NodeID":"review"`)
	})

	t.Run("caller not the assignee is rejected", func(t *testing.T) {
		svc, instances, tasks, _, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task

		_, err := svc.Complete(context.Background(), tenantID, task.ID, uuid.New(), json.RawMessage(`{}`), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrNotAssignee)
	})

	t.Run("connector-typed task is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		connectorType := "send-email"
		task.ConnectorType = &connectorType
		tasks.byID[task.ID] = task

		_, err := svc.Complete(context.Background(), tenantID, task.ID, uuid.New(), json.RawMessage(`{}`), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrTaskNotHumanActionable)
	})

	t.Run("assignee unavailable is rejected", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		svc := &service.TaskService{
			Instances: instances, Tasks: tasks, Assignments: assignments,
			Temporal: &fakeTemporalClient{}, IAM: &fakeIAMClient{status: port.UserStatus{IsDeleted: true}},
		}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{}`), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrAssigneeUnavailable)
	})

	t.Run("IAM check failure fails open", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		tasks := newFakeTaskRepo()
		assignments := newFakeAssignmentRepo()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		log := &fakeLogger{}
		svc := &service.TaskService{
			Instances: instances, Tasks: tasks, Assignments: assignments,
			Temporal: &fakeTemporalClient{}, IAM: &fakeIAMClient{err: errors.New("boom")}, Log: log,
		}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{}`), task.RecordVersion)
		assert.NoError(t, err, "an IAM check failure must fail open, not block the action")
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("multi-assignee lead may complete", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		leadID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: leadID, IsActive: true, IsLead: true}
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, leadID, json.RawMessage(`{}`), task.RecordVersion)
		require.NoError(t, err)
		assert.Len(t, temporal.signals, 1)
	})

	t.Run("multi-assignee non-lead caller is rejected", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		leadID, nonLeadID := uuid.New(), uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: leadID, IsActive: true, IsLead: true}
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: nonLeadID, IsActive: true}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, nonLeadID, json.RawMessage(`{}`), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrNotAssignee)
	})

	t.Run("multi-assignee with no lead yet is rejected", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		task.AssigneeMode = "all"
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{}`), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrNotAssignee)
	})

	t.Run("Instances.GetByID error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{}`), task.RecordVersion)
		assert.Error(t, err, "the instance was never seeded, so Instances.GetByID must fail")
	})

	t.Run("SignalWorkflow failure is wrapped and returned", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

		_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{}`), task.RecordVersion)
		assert.Error(t, err)
	})
}

func TestTaskService_Defer(t *testing.T) {
	t.Run("success signals stage-defer", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.Defer(context.Background(), tenantID, task.ID, userID, "not my department", task.RecordVersion)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, "stage-defer", temporal.signals[0].SignalName)
	})

	t.Run("connector-typed task is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		connectorType := "send-email"
		task.ConnectorType = &connectorType
		tasks.byID[task.ID] = task

		_, err := svc.Defer(context.Background(), tenantID, task.ID, uuid.New(), "reason", task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrTaskNotHumanActionable)
	})

	t.Run("record version conflict", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		tasks.byID[task.ID] = task

		_, err := svc.Defer(context.Background(), tenantID, task.ID, uuid.New(), "reason", 99)
		assert.ErrorIs(t, err, port.ErrRecordVersionConflict)
	})

	t.Run("invalid state", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.Status = domain.TaskStatusCompleted
		tasks.byID[task.ID] = task

		_, err := svc.Defer(context.Background(), tenantID, task.ID, uuid.New(), "reason", task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrInvalidTaskState)
	})

	t.Run("caller not the assignee is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		tasks.byID[task.ID] = task

		_, err := svc.Defer(context.Background(), tenantID, task.ID, uuid.New(), "reason", task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrNotAssignee)
	})

	t.Run("Instances.GetByID error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.Defer(context.Background(), tenantID, task.ID, userID, "reason", task.RecordVersion)
		assert.Error(t, err)
	})

	t.Run("SignalWorkflow failure is wrapped and returned", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

		_, err := svc.Defer(context.Background(), tenantID, task.ID, userID, "reason", task.RecordVersion)
		assert.Error(t, err)
	})
}

func TestTaskService_Reassign(t *testing.T) {
	t.Run("success signals instance-reassign", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		oldUserID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldUserID, IsActive: true}
		newUserID := uuid.New()

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), newUserID, task.RecordVersion)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, temporal.signals[0].SignalName)
		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), oldUserID.String())
		assert.Contains(t, string(b), newUserID.String())
	})

	t.Run("connector-typed task is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		connectorType := "send-email"
		task.ConnectorType = &connectorType
		tasks.byID[task.ID] = task

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrTaskNotHumanActionable)
	})

	t.Run("record version conflict", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		tasks.byID[task.ID] = task

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), 99)
		assert.ErrorIs(t, err, port.ErrRecordVersionConflict)
	})

	t.Run("invalid state", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		task.Status = domain.TaskStatusCompleted
		tasks.byID[task.ID] = task

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrInvalidTaskState)
	})

	t.Run("no-op reassignment to the same user is rejected", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), userID, task.RecordVersion)
		assert.ErrorIs(t, err, port.ErrOverrideNoOp)
	})

	t.Run("ListActiveByTask error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID := uuid.New()
		task := newReadyTask(tenantID, uuid.New())
		tasks.byID[task.ID] = task
		assignments.listActiveByTaskErr = assert.AnError

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), task.RecordVersion)
		assert.Error(t, err)
	})

	t.Run("Instances.GetByID error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		oldUserID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldUserID, IsActive: true}

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), task.RecordVersion)
		assert.Error(t, err)
	})

	t.Run("SignalWorkflow failure is wrapped and returned", func(t *testing.T) {
		svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		oldUserID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldUserID, IsActive: true}
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

		_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), task.RecordVersion)
		assert.Error(t, err)
	})
}

func TestTaskService_OverrideAssignee(t *testing.T) {
	t.Run("success persists the audit row and grafts the task's record_version", func(t *testing.T) {
		svc, _, tasks, assignments, overrides, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		task.RecordVersion = 4
		tasks.byID[task.ID] = task
		previousUserID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: previousUserID, IsActive: true}
		newUserID := uuid.New()

		got, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review",
			NewUserID: newUserID, Reason: "on leave", ActorUserID: uuid.New(), RecordVersion: 4,
		})
		require.NoError(t, err)
		assert.Equal(t, previousUserID, got.PreviousUserID)
		assert.Equal(t, newUserID, got.NewUserID)
		assert.Equal(t, int64(4), got.RecordVersion)
		assert.Len(t, overrides.created, 1)
	})

	t.Run("already resolved node is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		task.Status = domain.TaskStatusCompleted
		tasks.byID[task.ID] = task

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: uuid.New(), RecordVersion: task.RecordVersion,
		})
		assert.ErrorIs(t, err, port.ErrNodeAlreadyResolved)
	})

	t.Run("stale record_version is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: uuid.New(), RecordVersion: 99,
		})
		assert.ErrorIs(t, err, port.ErrRecordVersionConflict)
	})

	t.Run("overriding to the same user is a no-op", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: userID, RecordVersion: task.RecordVersion,
		})
		assert.ErrorIs(t, err, port.ErrOverrideNoOp)
	})

	t.Run("node not found", func(t *testing.T) {
		svc, _, _, _, _, _ := newTaskServiceHarness()
		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: uuid.New(), InstanceID: uuid.New(), NodeKey: "finance/does-not-exist", NewUserID: uuid.New(),
		})
		assert.ErrorIs(t, err, port.ErrTaskNotFound)
	})

	t.Run("connector-typed task is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		connectorType := "send-email"
		task.ConnectorType = &connectorType
		tasks.byID[task.ID] = task

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: uuid.New(), RecordVersion: task.RecordVersion,
		})
		assert.ErrorIs(t, err, port.ErrTaskNotHumanActionable)
	})

	t.Run("ListActiveByTask error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		assignments.listActiveByTaskErr = assert.AnError

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: uuid.New(), RecordVersion: task.RecordVersion,
		})
		assert.Error(t, err)
	})

	t.Run("no active assignee is rejected", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: uuid.New(), RecordVersion: task.RecordVersion,
		})
		assert.ErrorIs(t, err, port.ErrNotAssignee)
	})

	t.Run("Overrides.Create error propagates", func(t *testing.T) {
		svc, _, tasks, assignments, overrides, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		previousUserID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: previousUserID, IsActive: true}
		overrides.createErr = assert.AnError

		_, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: "finance/review", NewUserID: uuid.New(), RecordVersion: task.RecordVersion,
		})
		assert.Error(t, err)
	})
}

func TestTaskService_SignalReassign(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		svc, instances, tasks, _, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task

		err := svc.SignalReassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), uuid.New(), task.RecordVersion)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, temporal.signals[0].SignalName)
	})

	t.Run("Tasks.GetByID error propagates", func(t *testing.T) {
		svc, _, _, _, _, _ := newTaskServiceHarness()
		err := svc.SignalReassign(context.Background(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), 1)
		assert.Error(t, err)
	})

	t.Run("Instances.GetByID error propagates", func(t *testing.T) {
		svc, _, tasks, _, _, _ := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task

		err := svc.SignalReassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), uuid.New(), task.RecordVersion)
		assert.Error(t, err)
	})

	t.Run("SignalWorkflow failure is wrapped and returned", func(t *testing.T) {
		svc, instances, tasks, _, _, temporal := newTaskServiceHarness()
		tenantID, instanceID := uuid.New(), uuid.New()
		instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz"}
		task := newReadyTask(tenantID, instanceID)
		tasks.byID[task.ID] = task
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

		err := svc.SignalReassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), uuid.New(), task.RecordVersion)
		assert.Error(t, err)
	})
}
