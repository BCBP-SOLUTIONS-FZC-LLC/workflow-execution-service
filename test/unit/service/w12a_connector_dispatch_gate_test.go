package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// w12aGateTasks answers only for the tenant that owns the row, as row-level security does,
// records every read, and fails the reads it is told to.
type w12aGateTasks struct {
	*fakeTaskRepo
	reads   []w12aLookup
	readErr error
}

func (r *w12aGateTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w12aLookup{tenantID: tenantID, id: id})
	if r.readErr != nil {
		return nil, r.readErr
	}
	task, ok := r.byID[id]
	if !ok || task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return task, nil
}

func w12aNewGate() (*service.ConnectorDispatchGate, *w12aGateTasks, *w12aOOOInstances) {
	tasks := &w12aGateTasks{fakeTaskRepo: newFakeTaskRepo()}
	instances := &w12aOOOInstances{fakeInstanceRepo: newFakeInstanceRepo()}
	return &service.ConnectorDispatchGate{Instances: instances, Tasks: tasks}, tasks, instances
}

func TestConnectorDispatchGate_ATaskNotYetVisible_IsDispatchableAndTheLaterCommitDecides(t *testing.T) {
	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	inst := instanceWithStatus(task, domain.InstanceStatusRunning)
	inst.TenantID = task.TenantID
	stranger := uuid.New()

	gate, tasks, instances := w12aNewGate()
	instances.byID[inst.ID] = inst

	t.Run("before the task is committed", func(t *testing.T) {
		got, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

		require.NoError(t, err)
		assert.True(t, got, "the callback refuses an unknown task later; refusing here would hide one that is only late to commit")
		assert.Equal(t, []w12aLookup{{tenantID: task.TenantID, id: task.ID}}, tasks.reads, "one read, for this tenant and task")
		assert.Empty(t, instances.reads, "there is no instance to read for a task that is not there")
	})

	tasks.byID[task.ID] = task
	tasks.reads = nil

	t.Run("once committed as open it is still dispatchable and the instance is read", func(t *testing.T) {
		got, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

		require.NoError(t, err)
		assert.True(t, got)
		assert.Equal(t, []w12aLookup{{tenantID: task.TenantID, id: inst.ID}}, instances.reads)
	})

	t.Run("another tenant's task is as good as unknown", func(t *testing.T) {
		tasks.reads, instances.reads = nil, nil

		got, err := gate.Dispatchable(context.Background(), stranger, task.ID)

		require.NoError(t, err)
		assert.True(t, got)
		assert.Equal(t, []w12aLookup{{tenantID: stranger, id: task.ID}}, tasks.reads)
		assert.Empty(t, instances.reads, "nothing of the other tenant's instance is read")
	})

	t.Run("once committed as finished it is refused", func(t *testing.T) {
		task.Status = domain.TaskStatusCompleted
		tasks.reads, instances.reads = nil, nil

		got, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

		require.NoError(t, err)
		assert.False(t, got)
		assert.Empty(t, instances.reads)
	})
}

func TestConnectorDispatchGate_ATaskReadThatFailsForAnotherReason_IsNotTreatedAsUnknown(t *testing.T) {
	gate, tasks, instances := w12aNewGate()
	tasks.readErr = errW12aStoreDown
	taskID, tenantID := uuid.New(), uuid.New()

	got, err := gate.Dispatchable(context.Background(), tenantID, taskID)

	require.ErrorIs(t, err, errW12aStoreDown, "a store failure is returned so nothing runs on a guess")
	assert.NotErrorIs(t, err, port.ErrTaskNotFound)
	assert.False(t, got)
	assert.Equal(t, []w12aLookup{{tenantID: tenantID, id: taskID}}, tasks.reads)
	assert.Empty(t, instances.reads)
}
