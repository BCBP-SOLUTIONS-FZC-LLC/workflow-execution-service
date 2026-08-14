package temporal_test

import (
	"context"
	"encoding/json"
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

// TestPauseInstance_RetriedCall_NoOp is the regression test for the
// retry-forever finding: PauseInstance no longer trusts in.RecordVersion —
// a retry (instance already Paused from a prior attempt, current
// RecordVersion now past the stale input value) must no-op rather than
// hit a version conflict and retry forever.
func TestPauseInstance_RetriedCall_NoOp(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusRunning}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	in := port.PauseInstanceInput{InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1}

	require.NoError(t, deps.PauseInstance(context.Background(), in))
	require.NoError(t, deps.PauseInstance(context.Background(), in), "a retried PauseInstance must succeed idempotently, not error")

	assert.Equal(t, domain.InstanceStatusPaused, inst.Status)
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.instance.paused")
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

// TestUpdateInstanceStatus_Degraded_EnqueuesDegradedEvent is the regression
// test for the missing-DEGRADED-event finding: this transition must now
// build and enqueue workflow.instance.degraded, carrying the FailedBranches
// internal/workflow/degraded.go passes in.
func TestUpdateInstanceStatus_Degraded_EnqueuesDegradedEvent(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)
	deptID := uuid.New()

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusDegraded,
		FailedBranches: []domain.FailedBranch{{DepartmentID: deptID, LastNodeKey: "legal_review/approve"}},
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusDegraded, inst.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowInstanceDegraded, outbox.enqueued[0].Type)

	var payload domain.WorkflowInstanceDegradedPayload
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	require.Len(t, payload.FailedBranches, 1)
	assert.Equal(t, deptID, payload.FailedBranches[0].DepartmentID)
	assert.Equal(t, "legal_review/approve", payload.FailedBranches[0].LastNodeKey)
}

// TestUpdateInstanceStatus_Degraded_RetriedCall_NoOp is the idempotency
// regression test for the same fix: a retried call after the instance is
// already Degraded must no-op rather than re-enqueuing a duplicate event.
func TestUpdateInstanceStatus_Degraded_RetriedCall_NoOp(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusDegraded}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusDegraded,
	})
	require.NoError(t, err)
	assert.Empty(t, outbox.enqueued, "retry must not re-enqueue workflow.instance.degraded")
}

// TestUpdateInstanceStatus_Running_EnqueuesDegradedRecoveryResumed is the
// regression test for the Degraded->Running recovery half of the same
// finding: this is the only path that ever reaches this activity with
// Status=Running (a plain resume goes through ResumeInstanceActivity
// instead), and must now reuse workflow.instance.resumed with
// initiator=degraded_recovery rather than staying silent.
func TestUpdateInstanceStatus_Running_EnqueuesDegradedRecoveryResumed(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	startedBy := uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusDegraded, StartedByUserID: startedBy}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusRunning,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowInstanceResumed, outbox.enqueued[0].Type)

	var payload domain.WorkflowInstanceResumedPayload
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	assert.Equal(t, domain.InitiatorDegradedRecovery, payload.Initiator)
	assert.Equal(t, startedBy, payload.StartedByUserID)
	assert.Nil(t, payload.ActorUserID)
}
