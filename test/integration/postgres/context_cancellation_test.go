//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// TestRepositoryMethods_SurfaceUnderlyingErrors exercises each repo's generic
// mapErr(err) passthrough branch by cancelling ctx after a transaction is
// already open (so the failure surfaces from the query itself, past the
// already-covered connection-acquisition path).
func TestRepositoryMethods_SurfaceUnderlyingErrors(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)

	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	processedEventRepo := postgres.NewProcessedEventRepo(appPool)
	transactor := postgres.NewTransactor(appPool)

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(context.Background(), inst))
	task := newTask(tenantA, inst.ID)
	require.NoError(t, taskRepo.Create(context.Background(), task))
	assignment := newTaskAssignment(tenantA, task.ID, uuid.New())
	require.NoError(t, assignmentRepo.Create(context.Background(), assignment))

	withCancelledTx := func(fn func(ctx context.Context) error) error {
		return transactor.RunInTx(withGUC(context.Background(), tenantA), func(txCtx context.Context) error {
			cancelled, cancel := context.WithCancel(txCtx)
			cancel()
			return fn(cancelled)
		})
	}

	t.Run("InstanceRepo.ListByTenant", func(t *testing.T) {
		err := withCancelledTx(func(ctx context.Context) error {
			_, _, err := instanceRepo.ListByTenant(ctx, tenantA, port.InstanceListFilter{}, port.PageRequest{})
			return err
		})
		assert.Error(t, err)
	})

	t.Run("TaskRepo.ListByInstance", func(t *testing.T) {
		err := withCancelledTx(func(ctx context.Context) error {
			_, _, err := taskRepo.ListByInstance(ctx, tenantA, inst.ID, port.PageRequest{})
			return err
		})
		assert.Error(t, err)
	})

	t.Run("TaskAssignmentRepo", func(t *testing.T) {
		err := withCancelledTx(func(ctx context.Context) error {
			_, err := assignmentRepo.ListActiveByTask(ctx, tenantA, task.ID)
			return err
		})
		assert.Error(t, err)

		err = withCancelledTx(func(ctx context.Context) error {
			_, err := assignmentRepo.ListActiveByUser(ctx, tenantA, assignment.UserID)
			return err
		})
		assert.Error(t, err)

		err = withCancelledTx(func(ctx context.Context) error {
			_, err := assignmentRepo.Vacate(ctx, tenantA, assignment.ID)
			return err
		})
		assert.Error(t, err)
	})

	t.Run("OutboxRepo.ListByInstance", func(t *testing.T) {
		err := withCancelledTx(func(ctx context.Context) error {
			_, _, err := outboxRepo.ListByInstance(ctx, tenantA, inst.ID, port.PageRequest{})
			return err
		})
		assert.Error(t, err)
	})

	t.Run("ProcessedEventRepo", func(t *testing.T) {
		err := withCancelledTx(func(ctx context.Context) error {
			_, err := processedEventRepo.RecordIfNew(ctx, uuid.New(), "membership-execution", "x")
			return err
		})
		assert.Error(t, err)

		err = withCancelledTx(func(ctx context.Context) error {
			_, err := processedEventRepo.PruneOlderThan(ctx, 7*24*time.Hour)
			return err
		})
		assert.Error(t, err)
	})
}
