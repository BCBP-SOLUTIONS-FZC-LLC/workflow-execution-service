//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var errW16aVacateRefused = errors.New("w16a: vacate refused")

type w16aVacateCall struct{ tenantID, assignmentID uuid.UUID }

// w16aFailingVacate lets the first vacate through to the real repository and fails every later one.
type w16aFailingVacate struct {
	port.TaskAssignmentRepository
	calls []w16aVacateCall
}

func (r *w16aFailingVacate) Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.calls = append(r.calls, w16aVacateCall{tenantID, id})
	if len(r.calls) > 1 {
		return nil, errW16aVacateRefused
	}
	return r.TaskAssignmentRepository.Vacate(ctx, tenantID, id)
}

type w16aTerminateRecorder struct {
	port.TemporalClient
	terminated []string
}

func (r *w16aTerminateRecorder) TerminateWorkflow(_ context.Context, workflowID, _, _ string) error {
	r.terminated = append(r.terminated, workflowID)
	return nil
}

type w16aTerminateWorld struct {
	tenantID    uuid.UUID
	instance    *domain.Instance
	tasks       []*domain.Task
	assignments []*domain.TaskAssignment
}

func w16aSeedTerminateWorld(t *testing.T, repos w16aRepos) *w16aTerminateWorld {
	t.Helper()
	ctx := context.Background()
	w := &w16aTerminateWorld{tenantID: uuid.New()}
	w.instance = newInstance(w.tenantID, time.Now().UTC())
	require.NoError(t, repos.instances.Create(ctx, w.instance))
	for i := 0; i < 2; i++ {
		task := newTask(w.tenantID, w.instance.ID)
		task.Status = domain.TaskStatusInProgress
		require.NoError(t, repos.tasks.Create(ctx, task))
		a := newTaskAssignment(w.tenantID, task.ID, uuid.New())
		require.NoError(t, repos.assignments.Create(ctx, a))
		w.tasks = append(w.tasks, task)
		w.assignments = append(w.assignments, a)
	}
	return w
}

type w16aRepos struct {
	instances   *postgres.InstanceRepo
	tasks       *postgres.TaskRepo
	assignments *postgres.TaskAssignmentRepo
	outbox      *postgres.OutboxRepo
}

func TestInstanceService_Terminate_AFailureInTheCascadeLeavesEverythingAsItWasInPostgres(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repos := w16aRepos{
		instances: postgres.NewInstanceRepo(appPool), tasks: postgres.NewTaskRepo(appPool),
		assignments: postgres.NewTaskAssignmentRepo(appPool), outbox: postgres.NewOutboxRepo(appPool),
	}
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	ctx := context.Background()

	outboxRows := func(t *testing.T, tenantID uuid.UUID) int {
		t.Helper()
		var n int
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE tenant_id::text = $1", tenantID.String()).Scan(&n)
		}))
		return n
	}
	newService := func(assignments port.TaskAssignmentRepository, temporal port.TemporalClient) *service.InstanceService {
		return &service.InstanceService{
			Instances: repos.instances, Tasks: repos.tasks, Assignments: assignments, Outbox: repos.outbox,
			Transactor: postgres.NewTransactor(appPool), Temporal: temporal, Validator: validator,
		}
	}

	t.Run("a vacate that fails after another task was already failed and vacated rolls all of it back", func(t *testing.T) {
		w := w16aSeedTerminateWorld(t, repos)
		failing := &w16aFailingVacate{TaskAssignmentRepository: repos.assignments}
		temporal := &w16aTerminateRecorder{}

		err := newService(failing, temporal).Terminate(ctx, w.tenantID, w.instance.ID, uuid.New(), "cleanup")

		require.ErrorIs(t, err, errW16aVacateRefused)
		require.NotEmpty(t, failing.calls)
		for _, c := range failing.calls {
			assert.Equal(t, w.tenantID, c.tenantID)
		}
		assert.Equal(t, []string{w.instance.TemporalWorkflowID}, temporal.terminated, "the workflow was stopped once, before the transaction, and not again")

		stored, err := repos.instances.GetByID(ctx, w.tenantID, w.instance.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusRunning, stored.Status, "the status change was rolled back")
		assert.Equal(t, w.instance.RecordVersion, stored.RecordVersion)
		for i, task := range w.tasks {
			got, err := repos.tasks.GetByID(ctx, w.tenantID, task.ID)
			require.NoError(t, err)
			assert.Equal(t, domain.TaskStatusInProgress, got.Status, "task %d keeps its status", i)
			assert.Equal(t, task.RecordVersion, got.RecordVersion)
			active, err := repos.assignments.ListActiveByTask(ctx, w.tenantID, task.ID)
			require.NoError(t, err)
			require.Len(t, active, 1, "task %d keeps its active assignment", i)
			assert.Equal(t, w.assignments[i].ID, active[0].ID)
		}
		assert.Zero(t, outboxRows(t, w.tenantID), "no event was written")
	})

	t.Run("control: the same instance with nothing failing is terminated, its tasks failed and its events written", func(t *testing.T) {
		w := w16aSeedTerminateWorld(t, repos)

		require.NoError(t, newService(repos.assignments, &w16aTerminateRecorder{}).Terminate(ctx, w.tenantID, w.instance.ID, uuid.New(), "cleanup"))

		stored, err := repos.instances.GetByID(ctx, w.tenantID, w.instance.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusTerminated, stored.Status)
		for _, task := range w.tasks {
			got, err := repos.tasks.GetByID(ctx, w.tenantID, task.ID)
			require.NoError(t, err)
			assert.Equal(t, domain.TaskStatusFailed, got.Status)
			active, err := repos.assignments.ListActiveByTask(ctx, w.tenantID, task.ID)
			require.NoError(t, err)
			assert.Empty(t, active)
		}
		assert.Equal(t, 3, outboxRows(t, w.tenantID), "one task-failed event per task and the instance event")
	})
}
