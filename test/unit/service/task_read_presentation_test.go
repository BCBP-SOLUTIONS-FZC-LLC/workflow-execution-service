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

type trpWorld struct {
	tenantID, instanceID uuid.UUID
	user, service, empty *domain.Task
	assignments          *fakeAssignmentRepo
}

// newTrpWorld has a user task with two active assignments and one ended, a connector task with one,
// and a user task with none.
func newTrpWorld(tasks *fakeTaskRepo, assignments *fakeAssignmentRepo) *trpWorld {
	w := &trpWorld{tenantID: uuid.New(), instanceID: uuid.New(), assignments: assignments}
	connectorType := "send-email"
	mk := func(node string) *domain.Task {
		task := newReadyTask(w.tenantID, w.instanceID)
		task.NodeKey = node
		tasks.byID[task.ID] = task
		return task
	}
	w.user, w.service, w.empty = mk("finance/review"), mk("finance/notify"), mk("finance/audit")
	w.service.ConnectorType = &connectorType
	add := func(task *domain.Task, active bool) {
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: w.tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: active}
		assignments.byID[a.ID] = a
	}
	add(w.user, true)
	add(w.user, true)
	add(w.user, false)
	add(w.service, true)
	return w
}

func (w *trpWorld) wantType(task *domain.Task) string {
	if task.ConnectorType != nil {
		return "serviceTask"
	}
	return "userTask"
}

func (w *trpWorld) wantCount(task *domain.Task) int {
	switch task.ID {
	case w.user.ID:
		return 2
	case w.service.ID:
		return 1
	}
	return 0
}

func TestTaskList_EveryTaskCarriesItsTypeAndActiveAssigneeCount(t *testing.T) {
	svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
	w := newTrpWorld(tasks, assignments)

	res, err := svc.List(context.Background(), w.tenantID, port.ReadScope{IsAdmin: true}, port.TaskFilter{}, port.Page{Limit: 10})

	require.NoError(t, err)
	require.Len(t, res.Items, 3)
	for _, item := range res.Items {
		task := tasks.byID[item.ID]
		assert.Equal(t, w.wantType(task), item.TaskType, item.NodeKey)
		assert.Equal(t, w.wantCount(task), item.AssigneeCount, item.NodeKey)
	}
	require.Len(t, assignments.countCalls, 1, "one count query per page, not one per task")
	assert.ElementsMatch(t, []uuid.UUID{w.user.ID, w.service.ID, w.empty.ID}, assignments.countCalls[0])
}

func TestTaskList_CountQueryFails_ReturnsTheError(t *testing.T) {
	svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
	w := newTrpWorld(tasks, assignments)
	svc.Assignments = trpFailingCount{assignments}

	_, err := svc.List(context.Background(), w.tenantID, port.ReadScope{IsAdmin: true}, port.TaskFilter{}, port.Page{Limit: 10})

	require.ErrorIs(t, err, assert.AnError)
}

type trpFailingCount struct{ port.TaskAssignmentRepository }

func (trpFailingCount) CountActiveByTasks(context.Context, uuid.UUID, []uuid.UUID) (map[uuid.UUID]int, error) {
	return nil, assert.AnError
}

func TestTaskGet_CarriesItsTypeAndActiveAssigneeCount(t *testing.T) {
	svc, _, tasks, assignments, _, _ := newTaskServiceHarness()
	w := newTrpWorld(tasks, assignments)

	for _, task := range []*domain.Task{w.user, w.service, w.empty} {
		got, _, err := svc.Get(context.Background(), w.tenantID, task.ID, port.ReadScope{IsAdmin: true})

		require.NoError(t, err)
		assert.Equal(t, w.wantType(task), got.TaskType, task.NodeKey)
		assert.Equal(t, w.wantCount(task), got.AssigneeCount, task.NodeKey)
	}
}

func TestInstanceGet_EveryTaskCarriesItsTypeAndActiveAssigneeCount(t *testing.T) {
	svc, instances, tasks, assignments, _, _, _, _ := newInstanceServiceHarness()
	w := newTrpWorld(tasks, assignments)
	instances.byID[w.instanceID] = &domain.Instance{ID: w.instanceID, TenantID: w.tenantID, Status: domain.InstanceStatusRunning}

	_, got, err := svc.Get(context.Background(), w.tenantID, w.instanceID, port.ReadScope{IsAdmin: true})

	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, item := range got {
		task := tasks.byID[item.ID]
		assert.Equal(t, w.wantType(task), item.TaskType, item.NodeKey)
		assert.Equal(t, w.wantCount(task), item.AssigneeCount, item.NodeKey)
	}
	require.Len(t, assignments.countCalls, 1)
}
