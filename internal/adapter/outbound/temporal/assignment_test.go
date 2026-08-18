package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func newAssignmentTestDeps(tasks *fakeTaskRepo, assignments *fakeAssignmentRepo) (*outboundtemporal.Deps, *fakeOutbox) {
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks:       tasks,
		Assignments: assignments,
		Outbox:      outbox,
		Transactor:  fakeTransactor{},
		Validator:   noopValidator{},
	}
	return deps, outbox
}

func TestClaimAssignment_SetsLeadAndEnqueuesClaimed(t *testing.T) {
	tenantID, taskID := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(assignment))

	err := deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: tenantID.String(), UserID: assignment.UserID.String(), RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.True(t, assignment.IsLead)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskClaimed, outbox.enqueued[0].Type)
}

func TestCompleteAssignment_AllDoneWhenNoActiveAssignmentsRemain(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(assignment))

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(), ResultJSON: `{"decision":"approved"}`,
	})
	require.NoError(t, err)
	assert.True(t, out.AllDone)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskCompleted, outbox.enqueued[0].Type)
}

func TestCompleteAssignment_NotAllDoneWhenAnotherAssignmentStillActive(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	completing := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	stillActive := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(completing, stillActive))

	out, err := deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		AssignmentID: completing.ID.String(), TenantID: uuid.New().String(), ResultJSON: `{}`,
	})
	require.NoError(t, err)
	assert.False(t, out.AllDone)
}

func TestDeferTask_CreatesRegressionTaskForDeferrer(t *testing.T) {
	tenantID, deferrerID := uuid.New(), uuid.New()
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), NodeKey: "sales/review", AssigneeMode: "single", RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: deferrerID}
	tasks := newFakeTaskRepo(task)
	assignments := newFakeAssignmentRepo(assignment)
	deps, outbox := newAssignmentTestDeps(tasks, assignments)

	out, err := deps.DeferTask(context.Background(), port.DeferTaskInput{
		TaskID: taskID.String(), TenantID: tenantID.String(), UserID: deferrerID.String(),
		AssignmentID: assignment.ID.String(), Reason: "not ready yet",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusDeferred, task.Status)
	assert.False(t, assignment.IsActive)

	newTaskID, err := uuid.Parse(out.NewTaskID)
	require.NoError(t, err)
	newTask := tasks.byID[newTaskID]
	require.NotNil(t, newTask)
	assert.Equal(t, task.NodeKey, newTask.NodeKey)
	require.NotNil(t, newTask.DeferredFromTaskID)
	assert.Equal(t, taskID, *newTask.DeferredFromTaskID)

	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskDeferred, outbox.enqueued[0].Type)
}

// TestCompleteAssignment_RetriedCall_NoOp is the regression test for the
// missing-record-version-guard finding: a retried CompleteAssignment (the
// assignment already completed by a prior attempt whose ack was lost) must
// no-op rather than error or re-enqueue workflow.task.completed.
func TestCompleteAssignment_RetriedCall_NoOp(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(assignment))

	in := port.CompleteAssignmentInput{AssignmentID: assignment.ID.String(), TenantID: uuid.New().String(), ResultJSON: `{"decision":"approved"}`}

	out1, err := deps.CompleteAssignment(context.Background(), in)
	require.NoError(t, err)

	out2, err := deps.CompleteAssignment(context.Background(), in)
	require.NoError(t, err, "a retried CompleteAssignment must succeed idempotently, not error")
	assert.Equal(t, out1.AllDone, out2.AllDone)
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.task.completed")
}

// TestDeferTask_RetriedCall_IsIdempotent is the regression test for
// DeferTask's own missing-idempotency-key finding: a retry must resolve to
// the same regression task, not create a second one, and must not
// re-enqueue workflow.task.deferred.
func TestDeferTask_RetriedCall_IsIdempotent(t *testing.T) {
	tenantID, deferrerID := uuid.New(), uuid.New()
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), NodeKey: "sales/review", AssigneeMode: "single", RecordVersion: 1}
	assignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: deferrerID}
	tasks := newFakeTaskRepo(task)
	assignments := newFakeAssignmentRepo(assignment)
	deps, outbox := newAssignmentTestDeps(tasks, assignments)

	in := port.DeferTaskInput{
		TaskID: taskID.String(), TenantID: tenantID.String(), UserID: deferrerID.String(),
		AssignmentID: assignment.ID.String(), Reason: "not ready yet",
	}

	out1, err := deps.DeferTask(context.Background(), in)
	require.NoError(t, err)

	out2, err := deps.DeferTask(context.Background(), in)
	require.NoError(t, err, "a retried DeferTask must succeed idempotently, not error")

	assert.Equal(t, out1.NewTaskID, out2.NewTaskID, "retry must resolve to the same regression task")
	regressionTaskCount := 0
	for _, tk := range tasks.byID {
		if tk.DeferredFromTaskID != nil && *tk.DeferredFromTaskID == taskID {
			regressionTaskCount++
		}
	}
	assert.Equal(t, 1, regressionTaskCount, "retry must not create a second regression task")
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.task.deferred")
}

func TestReassignAssignment_VacatesOldInsertsNew(t *testing.T) {
	taskID, oldUser, newUser, adminUser := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	oldAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: oldUser}
	// A co-assignee on the same multi-assignee task, untouched by this
	// reassignment — exercises vacateAssignmentsFor's skip branch for an
	// active assignment that isn't the one being reassigned.
	coAssignee := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(oldAssignment, coAssignee))

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: oldUser.String(),
		NewUserID: newUser.String(), AdminUserID: adminUser.String(),
	})
	require.NoError(t, err)
	assert.False(t, oldAssignment.IsActive)
	assert.True(t, coAssignee.IsActive, "an active assignment for a different user must not be vacated")
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskReassigned, outbox.enqueued[0].Type)
}

// TestReassignAssignment_RetriedCall_NoOp is the regression test for the
// retry-forever finding: a retried ReassignAssignment (newUser already
// active from a prior attempt whose ack was lost) must no-op rather than
// hit uq_workflow_task_assignment_active and retry forever.
func TestReassignAssignment_RetriedCall_NoOp(t *testing.T) {
	taskID, oldUser, newUser, adminUser := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	oldAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: oldUser}
	assignments := newFakeAssignmentRepo(oldAssignment)
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), assignments)

	in := port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: oldUser.String(),
		NewUserID: newUser.String(), AdminUserID: adminUser.String(),
	}

	require.NoError(t, deps.ReassignAssignment(context.Background(), in))
	require.NoError(t, deps.ReassignAssignment(context.Background(), in), "a retried ReassignAssignment must succeed idempotently, not error")

	newUserActiveCount := 0
	for _, a := range assignments.byID {
		if a.UserID == newUser && a.IsActive {
			newUserActiveCount++
		}
	}
	assert.Equal(t, 1, newUserActiveCount, "retry must not create a second active assignment for newUser")
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.task.reassigned")
}

func TestUpdateTaskStatus_Failed_EnqueuesTaskFailed(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	assignee := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: uuid.New()}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(assignee))

	err := deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), Status: domain.TaskStatusFailed, RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailed, task.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskFailed, outbox.enqueued[0].Type)

	var payload domain.WorkflowTaskFailedPayload
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	assert.Equal(t, []uuid.UUID{assignee.UserID}, payload.AssigneeUserIDs, "assignee_user_ids must not be omitted — the schema requires it non-null")
}

func TestUpdateTaskStatus_NonFailed_NoEvent(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())

	err := deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), Status: domain.TaskStatusCompleted, RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, outbox.enqueued)
}

// TestUpdateTaskStatus_RetriedCall_NoOp is the regression test for the
// idempotency finding: a lost-ack Temporal retry replays the same
// (stale-by-then) in.RecordVersion. UpdateTaskStatus must refetch and no-op
// once the task is already in the target status, not reuse in.RecordVersion
// for the write and hit an unclassified, retried-forever version conflict.
func TestUpdateTaskStatus_RetriedCall_NoOp(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())

	in := port.UpdateTaskStatusInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), Status: domain.TaskStatusFailed, RecordVersion: 1,
	}
	require.NoError(t, deps.UpdateTaskStatus(context.Background(), in))
	require.NoError(t, deps.UpdateTaskStatus(context.Background(), in), "a retried UpdateTaskStatus must succeed idempotently, not error on the now-stale RecordVersion")
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.task.failed")
}
