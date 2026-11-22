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
)

func TestW21aInstanceStart_ContextThatIsNullOrEmpty_IsStoredAsAnEmptyObject(t *testing.T) {
	cases := []struct {
		name    string
		context json.RawMessage
		want    string
	}{
		{"null", json.RawMessage(`null`), `{}`},
		{"null with surrounding whitespace", json.RawMessage(" null\n"), `{}`},
		{"an empty object", json.RawMessage(`{}`), `{}`},
		{"an object with entries", json.RawMessage(`{"amount":10}`), `{"amount":10}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
			tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
			definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
			in := startfixInput(tenantID, versionID, caller)
			in.ContextJSON = tc.context

			got, err := svc.Start(context.Background(), in)

			require.NoError(t, err)
			stored := instances.byID[got.ID]
			require.NotNil(t, stored)
			assert.Equal(t, tenantID, stored.TenantID)
			assert.Equal(t, caller, stored.StartedByUserID)
			assert.JSONEq(t, tc.want, string(stored.ContextJSON), "the context the instance row holds")
			assert.JSONEq(t, tc.want, string(got.ContextJSON), "the context the start answers with")
		})
	}
}

func TestW21aTaskComplete_WithoutAResult_SignalsAnEmptyResultString(t *testing.T) {
	cases := []struct {
		name   string
		result json.RawMessage
		want   string
	}{
		{"no result at all", nil, ""},
		{"an empty result", json.RawMessage{}, ""},
		{"an object result", json.RawMessage(`{"decision":"approve"}`), `{"decision":"approve"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
			tenantID, instanceID, userID := uuid.New(), uuid.New(), uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
			task := newReadyTask(tenantID, instanceID)
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

			_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, tc.result, task.RecordVersion)

			require.NoError(t, err)
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, "stage-transition", temporal.signals[0].SignalName)
			assert.Equal(t, instanceID, temporal.signals[0].InstanceID)
			payload, err := json.Marshal(temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire map[string]any
			require.NoError(t, json.Unmarshal(payload, &wire))
			require.Contains(t, wire, "ResultJSON", "the signal always carries the field")
			assert.Equal(t, tc.want, wire["ResultJSON"])
			assert.Equal(t, userID.String(), wire["UserID"])
		})
	}
}

func TestW21aTaskReassign_Success_AnswersTheTaskWithItsTypeAndAssigneeCount(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
	fx9AllowEligibility(t, svc, uuid.Nil)
	tenantID, instanceID, oldUser, newUser, actor := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
	task := newReadyTask(tenantID, instanceID)
	task.RecordVersion = 6
	tasks.byID[task.ID] = task
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: oldUser, IsActive: true}

	got, err := svc.Reassign(context.Background(), tenantID, task.ID, actor, newUser, 6)

	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, tenantID, got.TenantID)
	assert.Equal(t, instanceID, got.WorkflowInstanceID)
	assert.Equal(t, "finance/review", got.NodeKey)
	assert.Equal(t, "userTask", got.TaskType)
	assert.Equal(t, port.TaskStatusReady, got.Status)
	assert.EqualValues(t, 6, got.RecordVersion)
	assert.Equal(t, "single", got.AssigneeMode)
	assert.Equal(t, 1, got.AssigneeCount, "the one active assignment is counted")
	assert.False(t, got.AssigneeCountUnavailable)
	require.Len(t, temporal.signals, 1)
	assert.Equal(t, instanceID, temporal.signals[0].InstanceID)
}

func TestW21aTaskReassign_StaleRecordVersion_SignalsNothing(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
	task := newReadyTask(tenantID, instanceID)
	task.RecordVersion = 6
	tasks.byID[task.ID] = task
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}

	got, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), uuid.New(), 5)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Nil(t, got)
	assert.Empty(t, temporal.signals, "a stale reassignment must not reach the workflow")
}
