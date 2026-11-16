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

func TestReassignAssignment_NobodyReplaced_EventOmitsOldUser(t *testing.T) {
	taskID, newUser := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(),
		NewUserID: newUser.String(), AdminUserID: uuid.NewString(), RecordVersion: 1,
	})

	require.NoError(t, err)
	require.Len(t, outbox.enqueued, 1)
	fields := wkactPayloadFields(t, outbox.enqueued[0])
	assert.NotContains(t, fields, "old_user_id", "no one was replaced, so the field is absent, not the all-zero id")
	assert.JSONEq(t, `"`+newUser.String()+`"`, string(fields["new_user_id"]))
}

func TestReassignAssignment_SomeoneReplaced_EventCarriesOldUser(t *testing.T) {
	taskID, oldUser, newUser := uuid.New(), uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	held := &domain.TaskAssignment{ID: uuid.New(), TaskID: taskID, UserID: oldUser}
	deps, outbox := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo(held))

	err := deps.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.New().String(), OldUserID: oldUser.String(),
		NewUserID: newUser.String(), AdminUserID: uuid.NewString(), RecordVersion: 1,
	})

	require.NoError(t, err)
	require.Len(t, outbox.enqueued, 1)
	fields := wkactPayloadFields(t, outbox.enqueued[0])
	assert.JSONEq(t, `"`+oldUser.String()+`"`, string(fields["old_user_id"]))
}
