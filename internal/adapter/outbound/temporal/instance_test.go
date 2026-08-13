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

func newInstanceTestDeps(inst *domain.Instance, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo) (*outboundtemporal.Deps, *fakeOutbox) {
	if tasks == nil {
		tasks = newFakeTaskRepo()
	}
	if assignments == nil {
		assignments = newFakeAssignmentRepo()
	}
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Instances:   newFakeInstanceRepo(inst),
		Tasks:       tasks,
		Assignments: assignments,
		Outbox:      outbox,
		Transactor:  fakeTransactor{},
		Validator:   noopValidator{},
	}
	return deps, outbox
}

func TestUpdateInstanceStatus_Completed_EnqueuesFinished(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, StartedByUserID: uuid.New()}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	now := time.Now().UTC()
	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusCompleted, CompletedAt: &now,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, inst.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowInstanceFinished, outbox.enqueued[0].Type)
}

func TestUpdateInstanceStatus_Failed_CascadesOpenTasksAndEnqueuesBoth(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	openTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, RecordVersion: 1}
	doneTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusCompleted, RecordVersion: 1}
	tasks := newFakeTaskRepo(openTask, doneTask)
	activeAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: openTask.ID, UserID: uuid.New()}
	assignments := newFakeAssignmentRepo(activeAssignment)

	deps, outbox := newInstanceTestDeps(inst, tasks, assignments)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusFailed,
	})
	require.NoError(t, err)

	assert.Equal(t, domain.TaskStatusFailed, openTask.Status, "open task cascades to FAILED")
	assert.Equal(t, domain.TaskStatusCompleted, doneTask.Status, "already-terminal task is untouched")
	assert.False(t, activeAssignment.IsActive, "the open task's assignment is vacated")

	require.Len(t, outbox.enqueued, 2, "one workflow.task.failed + one workflow.instance.failed")
	assert.Equal(t, domain.EventWorkflowTaskFailed, outbox.enqueued[0].Type)
	assert.Equal(t, domain.EventWorkflowInstanceFailed, outbox.enqueued[1].Type)
}

func TestPauseInstance_UpdatesStatusAndEnqueuesPaused(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusRunning}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusPaused, inst.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowInstancePaused, outbox.enqueued[0].Type)
}

func TestResumeInstance_UpdatesStatusAndEnqueuesResumed(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusPaused}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.ResumeInstance(context.Background(), port.ResumeInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowInstanceResumed, outbox.enqueued[0].Type)
}

func TestCancelInstance_CascadesAndTerminates(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	openTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusInProgress, RecordVersion: 1}
	tasks := newFakeTaskRepo(openTask)
	deps, outbox := newInstanceTestDeps(inst, tasks, newFakeAssignmentRepo())

	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailed, openTask.Status)
	assert.Equal(t, domain.InstanceStatusTerminated, inst.Status)
	require.Len(t, outbox.enqueued, 2)
	assert.Equal(t, domain.EventWorkflowTaskFailed, outbox.enqueued[0].Type)
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, outbox.enqueued[1].Type)
}

func TestUpdateInstanceStatus_Degraded_NoEventWritten(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusDegraded,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusDegraded, inst.Status)
	assert.Empty(t, outbox.enqueued, "Degraded has no input data to build a real payload from")
}
