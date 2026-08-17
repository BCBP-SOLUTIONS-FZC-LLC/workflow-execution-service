package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestToPortInstance(t *testing.T) {
	now := time.Now().UTC()
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: uuid.New(), WorkflowID: uuid.New(), WorkflowVersionID: uuid.New(),
		BusinessKey: "TND-001", TemporalWorkflowID: "wf-1", TemporalRunID: "run-1",
		Status: domain.InstanceStatusRunning, CurrentNodeKeys: []string{"review"}, SavedNodeKeys: []string{"approve"},
		ContextJSON: json.RawMessage(`{"a":1}`), OverrideMap: json.RawMessage(`{}`), TaskQueue: "wf-queue-default",
		StartedByUserID: uuid.New(), StartedAt: &now, CompletedAt: nil, RecordVersion: 3, CreatedAt: now, UpdatedAt: now,
	}
	got := toPortInstance(inst)
	assert.Equal(t, inst.ID, got.ID)
	assert.Equal(t, port.InstanceStatus(inst.Status), got.Status)
	assert.Equal(t, inst.BusinessKey, got.BusinessKey)
	assert.Equal(t, inst.CurrentNodeKeys, got.CurrentNodeKeys)
	assert.Equal(t, inst.ContextJSON, got.ContextJSON)
	assert.Equal(t, inst.RecordVersion, got.RecordVersion)
}

func TestToPortTask(t *testing.T) {
	connectorType := "send-email"
	task := &domain.Task{
		ID: uuid.New(), TenantID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "finance/review",
		DepartmentID: uuid.New(), Status: domain.TaskStatusReady, RecordVersion: 1, AssigneeMode: "single",
		ConnectorType: &connectorType, ExtrasJSON: json.RawMessage(`{"resolved_inputs":{}}`),
	}
	got := toPortTask(task)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, port.TaskStatus(task.Status), got.Status)
	assert.Equal(t, &connectorType, got.ConnectorType)
	assert.Equal(t, task.ExtrasJSON, got.ExtrasJSON)
	assert.Empty(t, got.TaskType, "TaskType has no real source yet -- must stay zero-valued, not fabricated")
	assert.Empty(t, got.RequiredLevel, "RequiredLevel has no real source yet -- must stay zero-valued, not fabricated")
}

func TestToPortTaskAssignment(t *testing.T) {
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: uuid.New(), TaskID: uuid.New(), UserID: uuid.New(), IsActive: true}
	got := toPortTaskAssignment(a)
	assert.Equal(t, a.ID, got.ID)
	assert.Equal(t, a.UserID, got.UserID)
	assert.True(t, got.IsActive)
}

func TestToPortActiveUserTask(t *testing.T) {
	row := port.ActiveUserTaskRow{
		TaskID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "finance/review",
		UserID: uuid.New(), DepartmentID: uuid.New(), Status: domain.TaskStatusReady, RecordVersion: 2,
	}
	got := toPortActiveUserTask(row)
	assert.Equal(t, row.TaskID, got.TaskID)
	assert.Equal(t, port.TaskStatus(row.Status), got.Status)
	assert.Equal(t, row.RecordVersion, got.RecordVersion)
}

func TestToPortAssigneeOverride(t *testing.T) {
	o := &domain.AssigneeOverride{
		ID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "finance/review",
		PreviousUserID: uuid.New(), NewUserID: uuid.New(), Reason: "on leave", ActorUserID: uuid.New(),
	}
	got := toPortAssigneeOverride(o, 5)
	assert.Equal(t, o.NewUserID, got.NewUserID)
	assert.Equal(t, int64(5), got.RecordVersion, "record_version is grafted from the task, not read off the override row")
}

func TestDomainEventTypeToPort(t *testing.T) {
	cases := []struct {
		domainType string
		want       port.WorkflowEventType
	}{
		{domain.EventWorkflowInstanceStarted, port.EventInstanceStarted},
		{domain.EventWorkflowInstancePaused, port.EventInstancePaused},
		{domain.EventWorkflowInstanceResumed, port.EventInstanceResumed},
		{domain.EventWorkflowInstanceCancelled, port.EventInstanceCancelled},
		{domain.EventWorkflowInstanceTerminated, port.EventInstanceTerminated},
		{domain.EventWorkflowInstanceFailed, port.EventInstanceFailed},
		{domain.EventWorkflowInstanceFinished, port.EventInstanceCompleted},
		{domain.EventWorkflowTaskCreated, port.EventTaskCreated},
		{domain.EventWorkflowTaskClaimed, port.EventTaskClaimed},
		{domain.EventWorkflowTaskCompleted, port.EventTaskCompleted},
		{domain.EventWorkflowTaskDeferred, port.EventTaskDeferred},
		{domain.EventWorkflowTaskReassigned, port.EventTaskReassigned},
		{domain.EventWorkflowTaskSuperseded, port.EventTaskSuperseded},
		{domain.EventWorkflowTaskFailed, port.EventTaskFailed},
		{domain.EventWorkflowInstanceForceRouted, port.EventInstanceForceRouted},
		{domain.EventWorkflowTaskSLAWarning, port.EventTaskSLAWarning},
		{domain.EventWorkflowTaskSLABreached, port.EventTaskSLABreached},
		{domain.EventWorkflowInstanceDegraded, port.EventInstanceDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.domainType, func(t *testing.T) {
			got, ok := domainEventTypeToPort(tc.domainType)
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("unrecognized type", func(t *testing.T) {
		_, ok := domainEventTypeToPort("workflow.something.unknown")
		assert.False(t, ok)
	})
}

func TestToWorkflowEvent(t *testing.T) {
	tenantID, instanceID := uuid.New(), uuid.New()

	t.Run("task-scoped event with actor", func(t *testing.T) {
		taskID := uuid.New()
		actorID := uuid.New()
		envelope := map[string]any{
			"tenant_id": tenantID.String(),
			"actor":     actorID.String(),
			"data": map[string]any{
				"task_id":  taskID.String(),
				"node_key": "finance/review",
			},
		}
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		rec := &domain.OutboxEventRecord{
			ID: uuid.New(), EventType: domain.EventWorkflowTaskCompleted, Payload: raw, CreatedAt: time.Now().UTC(),
		}

		got, err := toWorkflowEvent(rec, tenantID, instanceID)
		require.NoError(t, err)
		assert.Equal(t, port.EventTaskCompleted, got.EventType)
		require.NotNil(t, got.TaskID)
		assert.Equal(t, taskID, *got.TaskID)
		require.NotNil(t, got.NodeKey)
		assert.Equal(t, "finance/review", *got.NodeKey)
		require.NotNil(t, got.ActorUserID)
		assert.Equal(t, actorID, *got.ActorUserID)
		assert.Equal(t, instanceID, got.WorkflowInstanceID)
		assert.Equal(t, tenantID, got.TenantID)
	})

	t.Run("instance-scoped event has no task_id/node_key", func(t *testing.T) {
		envelope := map[string]any{"data": map[string]any{"started_by_user_id": uuid.New().String()}}
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		rec := &domain.OutboxEventRecord{ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted, Payload: raw, CreatedAt: time.Now().UTC()}

		got, err := toWorkflowEvent(rec, tenantID, instanceID)
		require.NoError(t, err)
		assert.Nil(t, got.TaskID)
		assert.Nil(t, got.NodeKey)
		assert.Nil(t, got.ActorUserID)
	})

	t.Run("malformed actor is silently ignored, not fatal", func(t *testing.T) {
		envelope := map[string]any{"actor": "not-a-uuid", "data": map[string]any{}}
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		rec := &domain.OutboxEventRecord{ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted, Payload: raw, CreatedAt: time.Now().UTC()}

		got, err := toWorkflowEvent(rec, tenantID, instanceID)
		require.NoError(t, err)
		assert.Nil(t, got.ActorUserID)
	})

	t.Run("unrecognized event type is an error", func(t *testing.T) {
		rec := &domain.OutboxEventRecord{ID: uuid.New(), EventType: "workflow.something.unknown", Payload: json.RawMessage(`{}`), CreatedAt: time.Now().UTC()}
		_, err := toWorkflowEvent(rec, tenantID, instanceID)
		assert.ErrorIs(t, err, errUnknownEventType)
	})

	t.Run("malformed envelope JSON is an error", func(t *testing.T) {
		rec := &domain.OutboxEventRecord{ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted, Payload: json.RawMessage(`not json`), CreatedAt: time.Now().UTC()}
		_, err := toWorkflowEvent(rec, tenantID, instanceID)
		assert.Error(t, err)
	})
}

func TestWrapInstanceErr(t *testing.T) {
	assert.NoError(t, wrapInstanceErr(nil))
	assert.ErrorIs(t, wrapInstanceErr(domain.ErrNotFound), port.ErrInstanceNotFound)
	assert.ErrorIs(t, wrapInstanceErr(domain.ErrRecordVersionConflict), port.ErrRecordVersionConflict)
	assert.ErrorIs(t, wrapInstanceErr(domain.ErrDuplicateBusinessKey), port.ErrDuplicateBusinessKey)

	other := assert.AnError
	assert.ErrorIs(t, wrapInstanceErr(other), other, "an unrecognized error passes through unchanged")
}

func TestWrapTaskErr(t *testing.T) {
	assert.NoError(t, wrapTaskErr(nil))
	assert.ErrorIs(t, wrapTaskErr(domain.ErrNotFound), port.ErrTaskNotFound)
	assert.ErrorIs(t, wrapTaskErr(domain.ErrRecordVersionConflict), port.ErrRecordVersionConflict)

	other := assert.AnError
	assert.ErrorIs(t, wrapTaskErr(other), other, "an unrecognized error passes through unchanged")
}
