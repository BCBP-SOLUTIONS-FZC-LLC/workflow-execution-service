package temporal_test

import (
	"context"
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

func TestReassignAssignment_VacatesOldInsertsNew(t *testing.T) {
	taskID, oldUser, newUser, adminUser := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	oldAssignment := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: oldUser}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(oldAssignment))

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: oldUser.String(),
		NewUserID: newUser.String(), AdminUserID: adminUser.String(),
	})
	require.NoError(t, err)
	assert.False(t, oldAssignment.IsActive)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskReassigned, outbox.enqueued[0].Type)
}

func TestUpdateTaskStatus_Failed_EnqueuesTaskFailed(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())

	err := deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), Status: domain.TaskStatusFailed, RecordVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailed, task.Status)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskFailed, outbox.enqueued[0].Type)
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
