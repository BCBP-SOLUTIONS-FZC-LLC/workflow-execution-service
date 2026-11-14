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
	run       func(port.AssigneeOverrideInput) (*port.AssigneeOverride, error)
}

func newTskfixOverrideWorld() *tskfixOverrideWorld {
	svc, _, tasks, assignments, overrides, _ := newTaskServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
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
		run: func(in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return svc.OverrideAssignee(context.Background(), in)
		},
	}
}

func TestTaskService_OverrideAssignee_RetryUsesTheSameOverrideID(t *testing.T) {
	w := newTskfixOverrideWorld()

	first, err := w.run(w.in)
	require.NoError(t, err)
	second, err := w.run(w.in)
	require.NoError(t, err)

	require.Len(t, w.overrides.created, 2)
	assert.Equal(t, w.overrides.created[0].ID, w.overrides.created[1].ID, "a retried override must carry the id the insert ignores")
	assert.Equal(t, first.ID, second.ID)
	assert.NotEqual(t, uuid.Nil, first.ID)
}

func TestTaskService_OverrideAssignee_IDIgnoresReasonAndActor(t *testing.T) {
	w := newTskfixOverrideWorld()
	first, err := w.run(w.in)
	require.NoError(t, err)

	retry := w.in
	retry.Reason = "different wording"
	retry.ActorUserID = uuid.New()
	second, err := w.run(retry)
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID)
}

func TestTaskService_OverrideAssignee_DistinctActionsGetDistinctIDs(t *testing.T) {
	base := newTskfixOverrideWorld()
	baseline, err := base.run(base.in)
	require.NoError(t, err)

	cases := map[string]func(*port.AssigneeOverrideInput){
		"another new user": func(in *port.AssigneeOverrideInput) { in.NewUserID = uuid.New() },
		"another version":  func(in *port.AssigneeOverrideInput) { in.RecordVersion = 5 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
			task := newReadyTask(base.in.TenantID, base.in.InstanceID)
			in := base.in
			mutate(&in)
			task.RecordVersion = in.RecordVersion
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: task.TenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}
			got, err := svc.OverrideAssignee(context.Background(), in)
			require.NoError(t, err)
			assert.NotEqual(t, baseline.ID, got.ID)
		})
	}
}
