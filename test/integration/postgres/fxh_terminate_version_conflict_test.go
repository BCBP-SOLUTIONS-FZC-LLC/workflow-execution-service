//go:build integration

package postgres_test

import (
	"context"
	"testing"

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

// fxhConcurrentBump commits another writer's version bump on the instance just before each of the
// first bumps status updates, as a concurrent request would.
type fxhConcurrentBump struct {
	port.InstanceRepository
	bump  func(ctx context.Context, instanceID uuid.UUID)
	bumps int
	calls int
}

func (r *fxhConcurrentBump) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
	r.calls++
	if r.calls <= r.bumps {
		r.bump(ctx, id)
	}
	return r.InstanceRepository.UpdateStatus(ctx, tenantID, id, status, version)
}

func TestInstanceService_Terminate_AConcurrentVersionBumpInPostgres(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repos := w16aRepos{
		instances: postgres.NewInstanceRepo(appPool), tasks: postgres.NewTaskRepo(appPool),
		assignments: postgres.NewTaskAssignmentRepo(appPool), outbox: postgres.NewOutboxRepo(appPool),
	}
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	ctx := context.Background()

	eventRows := func(t *testing.T, tenantID uuid.UUID) int {
		t.Helper()
		var n int
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE tenant_id::text = $1", tenantID.String()).Scan(&n)
		}))
		return n
	}
	newBumper := func(bumps int) *fxhConcurrentBump {
		return &fxhConcurrentBump{InstanceRepository: repos.instances, bumps: bumps, bump: func(ctx context.Context, instanceID uuid.UUID) {
			require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				_, err := conn.Exec(ctx, "UPDATE workflow_instance SET record_version = record_version + 1 WHERE id = $1", instanceID)
				return err
			}))
		}}
	}
	terminate := func(bumper *fxhConcurrentBump, w *w16aTerminateWorld) error {
		svc := &service.InstanceService{
			Instances: bumper, Tasks: repos.tasks, Assignments: repos.assignments, Outbox: repos.outbox,
			Transactor: postgres.NewTransactor(appPool), Temporal: &w16aTerminateRecorder{}, Validator: validator,
		}
		return svc.Terminate(ctx, w.tenantID, w.instance.ID, uuid.New(), "cleanup")
	}

	t.Run("one bump is retried and the instance is terminated with its events written once", func(t *testing.T) {
		w := w16aSeedTerminateWorld(t, repos)
		bumper := newBumper(1)

		require.NoError(t, terminate(bumper, w))

		assert.Equal(t, 2, bumper.calls)
		stored, err := repos.instances.GetByID(ctx, w.tenantID, w.instance.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusTerminated, stored.Status)
		assert.Equal(t, 3, eventRows(t, w.tenantID), "one task-failed event per task and the instance event")
	})

	t.Run("a bump on every attempt answers a version conflict after three attempts and writes nothing", func(t *testing.T) {
		w := w16aSeedTerminateWorld(t, repos)
		bumper := newBumper(100)

		err := terminate(bumper, w)

		require.ErrorIs(t, err, port.ErrRecordVersionConflict)
		assert.Equal(t, 3, bumper.calls)
		stored, err := repos.instances.GetByID(ctx, w.tenantID, w.instance.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusRunning, stored.Status)
		assert.Zero(t, eventRows(t, w.tenantID), "no event was written")
	})
}
