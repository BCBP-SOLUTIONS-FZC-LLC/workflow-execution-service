package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type tskfixOverrideWorld struct {
	in        port.AssigneeOverrideInput
	overrides *fakeAssigneeOverrideRepo
	outbox    *fakeOutbox
	run       func(port.AssigneeOverrideInput) (*port.AssigneeOverride, error)
}

func newTskfixOverrideWorld(t *testing.T) *tskfixOverrideWorld {
	svc, instances, tasks, assignments, overrides, _ := newTaskServiceHarness()
	fx9AllowEligibility(t, svc, uuid.Nil)
	outbox := &fakeOutbox{}
	svc.Signals = outbox
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
	task := newReadyTask(tenantID, instanceID)
	task.RecordVersion = 4
	tasks.byID[task.ID] = task
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}
	return &tskfixOverrideWorld{
		in: port.AssigneeOverrideInput{
			TenantID: tenantID, InstanceID: instanceID, NodeKey: task.NodeKey,
			NewUserID: uuid.New(), Reason: "on leave", ActorUserID: uuid.New(), RecordVersion: 4,
		},
		overrides: overrides,
		outbox:    outbox,
		run: func(in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return svc.OverrideAssignee(context.Background(), in)
		},
	}
}

func TestTaskService_OverrideAssignee_RetryOnTheOldVersionAfterTheCommit_AnswersConflictAndStoresNoSecondRow(t *testing.T) {
	w := newTskfixOverrideWorld(t)

	_, err := w.run(w.in)
	require.NoError(t, err)
	_, err = w.run(w.in)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Len(t, w.overrides.created, 1, "the retry writes no second audit row")
	assert.Len(t, w.outbox.signals, 1, "the move was queued with the first call, so the retry needs no second signal")
}
