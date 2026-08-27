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

// TestUpdateInstanceStatus_Paused_NoEventEnqueued covers the switch's
// Paused/Terminated case: neither status is ever reached through this
// activity in practice (each has its own dedicated Pause/CancelInstance
// activity), but the branch still exists and must stay a safe no-op rather
// than enqueuing a bogus event.
func TestUpdateInstanceStatus_Paused_NoEventEnqueued(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusRunning}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatusPaused,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusPaused, inst.Status, "the status write itself still happens")
	assert.Empty(t, outbox.enqueued, "no event class is defined for this activity reaching Paused")
}

// TestUpdateInstanceStatus_UnknownStatus_NoEventEnqueued covers the switch's
// default case for a status this activity doesn't know how to build an event
// for.
func TestUpdateInstanceStatus_UnknownStatus_NoEventEnqueued(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Status: domain.InstanceStatus("bogus_status"),
	})
	require.NoError(t, err)
	assert.Empty(t, outbox.enqueued)
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

// TestPauseInstance_NonAdminInitiator_CarriesThrough is the regression test
// for the initiator finding: a reconciler-triggered pause (tenant
// suspension, OOO, safety-net) must report its real initiator on the wire,
// not a hardcoded "admin".
func TestPauseInstance_NonAdminInitiator_CarriesThrough(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusRunning}
	deps, outbox := newInstanceTestDeps(inst, nil, nil)

	err := deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), Initiator: domain.InitiatorTenantState, RecordVersion: 1,
	})
	require.NoError(t, err)
	require.Len(t, outbox.enqueued, 1)

	var payload domain.WorkflowInstancePausedPayload
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	assert.Equal(t, domain.InitiatorTenantState, payload.Initiator)
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

// TestPauseInstance_GetInstanceError forces instanceLifecycleEvent's
// Instances.GetByID call (shared by Pause/ResumeInstance) to fail — the
// instance is absent from the repo.
func TestPauseInstance_GetInstanceError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(), Tasks: newFakeTaskRepo(), Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "get instance")
}

// TestResumeInstance_UpdateStatusError forces instanceLifecycleEvent's
// Instances.UpdateStatus call to fail once the instance is not already in
// the target status.
func TestResumeInstance_UpdateStatusError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1, Status: domain.InstanceStatusPaused}
	instances := newFakeInstanceRepo(inst)
	instances.updateStatusErr = errBoom
	deps := &outboundtemporal.Deps{
		Instances: instances, Tasks: newFakeTaskRepo(), Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	err := deps.ResumeInstance(context.Background(), port.ResumeInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), RecordVersion: 1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "update instance status")
}

// TestCancelInstance_CascadesAndEmitsCancelled is the regression test for
// the dead-event finding: cancel must emit workflow.instance.cancelled, not
// workflow.instance.terminated — the LLD documents these as distinct
// events — and must carry the caller-supplied reason through, not drop it.
func TestCancelInstance_CascadesAndEmitsCancelled(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	openTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusInProgress, RecordVersion: 1}
	tasks := newFakeTaskRepo(openTask)
	deps, outbox := newInstanceTestDeps(inst, tasks, newFakeAssignmentRepo())

	reason := "no longer needed"
	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), Reason: &reason, RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailed, openTask.Status)
	assert.Equal(t, domain.InstanceStatusTerminated, inst.Status, "the DB status stays TERMINATED - cancel/terminate diverge only at the event layer")
	require.Len(t, outbox.enqueued, 2)
	assert.Equal(t, domain.EventWorkflowTaskFailed, outbox.enqueued[0].Type)
	assert.Equal(t, domain.EventWorkflowInstanceCancelled, outbox.enqueued[1].Type)

	var payload domain.WorkflowInstanceCancelledPayload
	require.NoError(t, json.Unmarshal(outbox.enqueued[1].Payload, &payload))
	require.NotNil(t, payload.Reason)
	assert.Equal(t, reason, *payload.Reason)
}

// TestCancelInstance_GetInstanceError forces CancelInstance's
// Instances.GetByID call to fail — the instance is absent from the repo.
func TestCancelInstance_GetInstanceError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	deps := &outboundtemporal.Deps{
		Instances: newFakeInstanceRepo(), Tasks: newFakeTaskRepo(), Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	reason := "no longer needed"
	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), Reason: &reason, RecordVersion: 1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "get instance")
}

// TestCancelInstance_UpdateStatusError forces CancelInstance's
// Instances.UpdateStatus call to fail after the failActiveTasks cascade
// succeeds.
func TestCancelInstance_UpdateStatusError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	instances := newFakeInstanceRepo(inst)
	instances.updateStatusErr = errBoom
	deps := &outboundtemporal.Deps{
		Instances: instances, Tasks: newFakeTaskRepo(), Assignments: newFakeAssignmentRepo(),
		Outbox: &fakeOutbox{}, Transactor: fakeTransactor{}, Validator: noopValidator{},
	}

	reason := "no longer needed"
	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), Reason: &reason, RecordVersion: 1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "update instance status")
}

// TestFailActiveTasks_Paginates forces a second ListByInstance page during
// CancelInstance's task-failure cascade — open tasks spanning two pages must
// all cascade to FAILED, not just the first page's.
func TestFailActiveTasks_Paginates(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	page1Task := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, RecordVersion: 1}
	page2Task := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusInProgress, RecordVersion: 1}
	tasks := newFakeTaskRepo(page1Task, page2Task)
	tasks.pages = [][]*domain.Task{{page1Task}, {page2Task}}
	deps, outbox := newInstanceTestDeps(inst, tasks, newFakeAssignmentRepo())

	reason := "no longer needed"
	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), Reason: &reason, RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailed, page1Task.Status)
	assert.Equal(t, domain.TaskStatusFailed, page2Task.Status, "page 2's open task must cascade too")
	assert.Equal(t, 2, tasks.pageCalls, "the pagination loop must have followed the cursor to a second page")
	require.Len(t, outbox.enqueued, 3, "one workflow.task.failed per task, plus workflow.instance.cancelled")
}

// TestFailTask_ListActiveAssignmentsError forces failTask's
// Assignments.ListActiveByTask call to fail.
func TestFailTask_ListActiveAssignmentsError(t *testing.T) {
	instanceID, tenantID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	openTask := &domain.Task{ID: uuid.New(), WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady, RecordVersion: 1}
	tasks := newFakeTaskRepo(openTask)
	assignments := newFakeAssignmentRepo()
	assignments.listActiveErr = errBoom
	deps, _ := newInstanceTestDeps(inst, tasks, assignments)

	reason := "no longer needed"
	err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), AdminUserID: uuid.New().String(), Reason: &reason, RecordVersion: 1,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "list active assignments")
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
