package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func ingfixGate(task *domain.Task, inst *domain.Instance) (*service.ConnectorDispatchGate, *fakeInstanceRepo) {
	instances := newFakeInstanceRepo()
	if inst != nil {
		instances.byID[inst.ID] = inst
	}
	tasks := newFakeTaskRepo()
	if task != nil {
		tasks.byID[task.ID] = task
	}
	return &service.ConnectorDispatchGate{Instances: instances, Tasks: tasks}, instances
}

func TestConnectorDispatchGate_RunsOnlyForAnOpenTaskOnALiveInstance(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		task   domain.TaskStatus
		inst   domain.InstanceStatus
		expect bool
	}{
		{"ready on running", domain.TaskStatusReady, domain.InstanceStatusRunning, true},
		{"in progress on running", domain.TaskStatusInProgress, domain.InstanceStatusRunning, true},
		{"ready on degraded", domain.TaskStatusReady, domain.InstanceStatusDegraded, true},
		{"ready on paused (not decided here, still runs)", domain.TaskStatusReady, domain.InstanceStatusPaused, true},
		{"completed task", domain.TaskStatusCompleted, domain.InstanceStatusRunning, false},
		{"failed task", domain.TaskStatusFailed, domain.InstanceStatusRunning, false},
		{"superseded task", domain.TaskStatusSuperseded, domain.InstanceStatusRunning, false},
		{"deferred task", domain.TaskStatusDeferred, domain.InstanceStatusRunning, false},
		{"completed instance", domain.TaskStatusReady, domain.InstanceStatusCompleted, false},
		{"terminated instance", domain.TaskStatusReady, domain.InstanceStatusTerminated, false},
		{"failed instance", domain.TaskStatusReady, domain.InstanceStatusFailed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			task := connectorTask("storage", tc.task, `{}`)
			gate, _ := ingfixGate(task, instanceWithStatus(task, tc.inst))

			got, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

			require.NoError(t, err)
			assert.Equal(t, tc.expect, got)
		})
	}
}

func TestConnectorDispatchGate_AFinishedTaskNeedsNoInstanceRead(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusCompleted, `{}`)
	gate, instances := ingfixGate(task, nil)
	instances.getErr = errors.New("db down")

	got, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

	require.NoError(t, err)
	assert.False(t, got)
}

func TestConnectorDispatchGate_UnknownTaskStillRuns_TheCallbackRefusesIt(t *testing.T) {
	t.Parallel()

	gate, _ := ingfixGate(nil, nil)

	got, err := gate.Dispatchable(context.Background(), uuid.New(), uuid.New())

	require.NoError(t, err)
	assert.True(t, got)
}

func TestConnectorDispatchGate_ReadFailuresAreReturned(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	t.Run("instance read", func(t *testing.T) {
		t.Parallel()
		gate, instances := ingfixGate(task, instanceWithStatus(task, domain.InstanceStatusRunning))
		instances.getErr = errors.New("db down")

		_, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

		require.ErrorContains(t, err, "db down")
	})
	t.Run("unknown instance", func(t *testing.T) {
		t.Parallel()
		gate, _ := ingfixGate(task, nil)

		_, err := gate.Dispatchable(context.Background(), task.TenantID, task.ID)

		require.ErrorIs(t, err, port.ErrInstanceNotFound)
	})
}
