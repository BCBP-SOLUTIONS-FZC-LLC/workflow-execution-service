package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestReassignAssignment_NoPreviousUser_AssignsTheNewUserAndVacatesNobody(t *testing.T) {
	taskID, newUser, adminUser := uuid.New(), uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(),
		NewUserID: newUser.String(), AdminUserID: adminUser.String(), RecordVersion: 1,
	})

	require.NoError(t, err)
	active, listErr := deps.Assignments.ListActiveByTask(context.Background(), uuid.Nil, taskID)
	require.NoError(t, listErr)
	require.Len(t, active, 1)
	assert.Equal(t, newUser, active[0].UserID)
	assert.False(t, active[0].IsLead, "nobody held the lead to hand over")
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskReassigned, outbox.enqueued[0].Type)
}

func TestReassignAssignment_PreviousUserNotAnId_FailsValidation(t *testing.T) {
	taskID := uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: "not-a-uuid",
		NewUserID: uuid.NewString(), AdminUserID: uuid.NewString(), RecordVersion: 1,
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "parse old_user_id")
	assert.Empty(t, outbox.enqueued)
}
