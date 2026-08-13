package temporal_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestRecordForceRoute_SupersedesBypassedTaskAndEnqueuesBoth(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	bypassed := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/review", Status: domain.TaskStatusReady, RecordVersion: 1}
	unrelated := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, NodeKey: "sales/other", Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(bypassed, unrelated)
	activeAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: bypassed.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(activeAssignment)
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(inst), Tasks: tasks, Assignments: assignments,
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve", AdminUserID: uuid.New().String(),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusSuperseded, bypassed.Status)
	assert.Equal(t, domain.TaskStatusReady, unrelated.Status, "only the bypassed node's task is superseded")
	assert.False(t, activeAssignment.IsActive)

	require.Len(t, outbox.enqueued, 2)
	assert.Equal(t, domain.EventWorkflowTaskSuperseded, outbox.enqueued[0].Type)
	assert.Equal(t, domain.EventWorkflowInstanceForceRouted, outbox.enqueued[1].Type)
}

func TestRecordSLAWarning_EnqueuesAuditEventOnly(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	followUp := time.Now().UTC()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, FollowUpAt: &followUp}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusReady, task.Status, "audit-only: no status change")
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskSLAWarning, outbox.enqueued[0].Type)
}

func TestRecordSLABreach_EnqueuesAuditEventOnly(t *testing.T) {
	taskID, instanceID := uuid.New(), uuid.New()
	due := time.Now().UTC()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, DueAt: &due}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Outbox: outbox, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.RecordSLABreach(context.Background(), port.RecordSLABreachInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})
	require.NoError(t, err)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskSLABreached, outbox.enqueued[0].Type)
}
