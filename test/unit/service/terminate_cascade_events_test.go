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

type terminatedEventData struct {
	Initiator string  `json:"initiator"`
	Reason    *string `json:"reason"`
}

type taskFailedEventData struct {
	TaskID          string   `json:"task_id"`
	NodeKey         string   `json:"node_key"`
	DepartmentID    string   `json:"department_id"`
	AssigneeUserIDs []string `json:"assignee_user_ids"`
	CascadeSource   string   `json:"cascade_source"`
}

func TestInstanceService_Terminate_AnnouncesEachFailedTaskInTheSameTransaction(t *testing.T) {
	tenantID, actorID, assigneeID, deptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h := newCscHarness(inst)
	open := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "review", DepartmentID: deptID, Status: domain.TaskStatusReady, RecordVersion: 1}
	done := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "intake", DepartmentID: deptID, Status: domain.TaskStatusCompleted, RecordVersion: 3}
	h.tasks.byID[open.ID], h.tasks.byID[done.ID] = open, done
	assignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: open.ID, UserID: assigneeID, IsActive: true}
	h.assignments.byID[assignment.ID] = assignment

	require.NoError(t, h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "duplicate request"))

	require.Len(t, h.outbox.attempts, 2, "one task-failed event for the open task, then the instance event")
	failed := h.outbox.attempts[0]
	assert.Equal(t, domain.EventWorkflowTaskFailed, failed.Envelope.Type)
	assert.True(t, failed.InTx)
	assert.Equal(t, tenantID.String(), failed.Envelope.TenantID)
	assert.Equal(t, "instances/"+inst.ID.String(), failed.Envelope.Subject)
	var data taskFailedEventData
	require.NoError(t, json.Unmarshal(failed.Envelope.Payload, &data))
	assert.Equal(t, open.ID.String(), data.TaskID)
	assert.Equal(t, "review", data.NodeKey)
	assert.Equal(t, deptID.String(), data.DepartmentID)
	assert.Equal(t, []string{assigneeID.String()}, data.AssigneeUserIDs)
	assert.Equal(t, "instance_terminate", data.CascadeSource)
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, h.outbox.attempts[1].Envelope.Type)
}

func TestInstanceService_Terminate_CarriesTheReasonOnTheTerminatedEvent(t *testing.T) {
	tenantID, actorID := uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h := newCscHarness(inst)

	require.NoError(t, h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "duplicate request"))

	require.Len(t, h.outbox.attempts, 1)
	var data terminatedEventData
	require.NoError(t, json.Unmarshal(h.outbox.attempts[0].Envelope.Payload, &data))
	require.NotNil(t, data.Reason)
	assert.Equal(t, "duplicate request", *data.Reason)
	assert.Equal(t, domain.TerminatedInitiatorAdmin, data.Initiator)
}

func TestTenantLifecycleReconciler_Offboard_AnnouncesFailedTasksAndCarriesTheReason(t *testing.T) {
	svc, instances, tasks, assignments, _, outbox, _ := newTenantLifecycleHarness()
	tenantID, assigneeID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
	instances.byID[inst.ID] = inst
	open := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "review", Status: domain.TaskStatusInProgress, RecordVersion: 1}
	tasks.byID[open.ID] = open
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: open.ID, UserID: assigneeID, IsActive: true}
	assignments.byID[a.ID] = a

	err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"})
	require.NoError(t, err)

	require.Len(t, outbox.enqueued, 2)
	assert.Equal(t, domain.EventWorkflowTaskFailed, outbox.enqueued[0].Type)
	var failed taskFailedEventData
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &failed))
	assert.Equal(t, open.ID.String(), failed.TaskID)
	assert.Equal(t, []string{assigneeID.String()}, failed.AssigneeUserIDs)
	assert.Equal(t, "instance_terminate", failed.CascadeSource)
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, outbox.enqueued[1].Type)
	var terminated terminatedEventData
	require.NoError(t, json.Unmarshal(outbox.enqueued[1].Payload, &terminated))
	require.NotNil(t, terminated.Reason)
	assert.Equal(t, "tenant offboarded", *terminated.Reason)
}
