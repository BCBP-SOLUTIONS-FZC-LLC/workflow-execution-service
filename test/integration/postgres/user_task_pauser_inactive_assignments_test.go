//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type csfPauseSignal struct {
	WorkflowID string
	InstanceID uuid.UUID
	Name       string
}

type csfPauseTemporal struct {
	port.TemporalClient
	signals []csfPauseSignal
}

func (c *csfPauseTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, signalName string, _ any) error {
	c.signals = append(c.signals, csfPauseSignal{WorkflowID: workflowID, InstanceID: instanceID, Name: signalName})
	return nil
}

func TestPauseUserTasks_OnlyActiveAssignmentsSelectInstances(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	type seeded struct {
		pauser     *service.UserTaskPauser
		temporal   *csfPauseTemporal
		tenantID   uuid.UUID
		userID     uuid.UUID
		instance   *domain.Instance
		task       *domain.Task
		assignment *domain.TaskAssignment
	}
	seed := func(t *testing.T) seeded {
		t.Helper()
		s := seeded{tenantID: uuid.New(), userID: uuid.New(), temporal: &csfPauseTemporal{}}
		s.instance = newInstance(s.tenantID, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, s.instance))
		s.task = newTask(s.tenantID, s.instance.ID)
		require.NoError(t, taskRepo.Create(ctx, s.task))
		s.assignment = newTaskAssignment(s.tenantID, s.task.ID, s.userID)
		require.NoError(t, assignmentRepo.Create(ctx, s.assignment))
		s.pauser = &service.UserTaskPauser{
			Instances: instanceRepo, Assignments: assignmentRepo, Tasks: taskRepo, Temporal: s.temporal,
		}
		return s
	}

	t.Run("an active assignment pauses its running instance", func(t *testing.T) {
		s := seed(t)

		require.NoError(t, s.pauser.PauseUserTasks(ctx, s.tenantID, s.userID, nil))

		assert.Equal(t, []csfPauseSignal{{s.instance.TemporalWorkflowID, s.instance.ID, port.SignalInstancePause}}, s.temporal.signals)
	})

	t.Run("a vacated assignment pauses nothing", func(t *testing.T) {
		s := seed(t)
		_, err := assignmentRepo.Vacate(ctx, s.tenantID, s.assignment.ID)
		require.NoError(t, err)

		require.NoError(t, s.pauser.PauseUserTasks(ctx, s.tenantID, s.userID, nil))

		assert.Empty(t, s.temporal.signals)
	})

	t.Run("a completed assignment pauses nothing", func(t *testing.T) {
		s := seed(t)
		_, err := assignmentRepo.Complete(ctx, s.tenantID, s.assignment.ID, json.RawMessage(`{}`), s.task.RecordVersion)
		require.NoError(t, err)

		require.NoError(t, s.pauser.PauseUserTasks(ctx, s.tenantID, s.userID, nil))

		assert.Empty(t, s.temporal.signals)
	})

	t.Run("an assignment of another tenant is not read", func(t *testing.T) {
		s := seed(t)

		require.NoError(t, s.pauser.PauseUserTasks(ctx, uuid.New(), s.userID, nil))

		assert.Empty(t, s.temporal.signals)
	})
}
