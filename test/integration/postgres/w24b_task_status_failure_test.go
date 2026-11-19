//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskRepo_UpdateStatus_NotFoundAndDatabaseFailureAreDifferentAnswers(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()

	tenant := uuid.New()
	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	task := newTask(tenant, inst.ID)
	require.NoError(t, tasks.Create(ctx, task))

	t.Run("a task that does not exist is not found, not a version conflict", func(t *testing.T) {
		got, err := tasks.UpdateStatus(ctx, tenant, uuid.New(), domain.TaskStatusDeferred, 1)
		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.NotErrorIs(t, err, domain.ErrRecordVersionConflict)
		assert.Nil(t, got)
	})

	t.Run("another tenant's task is not found for the caller", func(t *testing.T) {
		got, err := tasks.UpdateStatus(ctx, uuid.New(), task.ID, domain.TaskStatusDeferred, task.RecordVersion)
		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.Nil(t, got)
	})

	t.Run("a statement the database refuses returns its error, no task, and rolls back the transaction", func(t *testing.T) {
		w8bSuperExec(t, superPool, `CREATE FUNCTION w24b_refuse_task_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'w24b: task update refused'; END $$`)
		w8bSuperExec(t, superPool, `CREATE TRIGGER w24b_refuse_task_update BEFORE UPDATE ON workflow_task FOR EACH ROW EXECUTE FUNCTION w24b_refuse_task_update()`)
		t.Cleanup(func() {
			w8bSuperExec(t, superPool, `DROP TRIGGER IF EXISTS w24b_refuse_task_update ON workflow_task`)
			w8bSuperExec(t, superPool, `DROP FUNCTION IF EXISTS w24b_refuse_task_update()`)
		})

		var got *domain.Task
		err := transactor.RunInTx(withGUC(ctx, tenant), func(txCtx context.Context) error {
			if _, err := instances.UpdateStatus(txCtx, tenant, inst.ID, domain.InstanceStatusPaused, inst.RecordVersion); err != nil {
				return err
			}
			var updateErr error
			got, updateErr = tasks.UpdateStatus(txCtx, tenant, task.ID, domain.TaskStatusDeferred, task.RecordVersion)
			return updateErr
		})

		require.Error(t, err)
		assert.True(t, pgcommon.IsPgError(err), "the database's own error reaches the caller: %v", err)
		w8bRequireNoClientAnswer(t, err)
		assert.Nil(t, got)

		storedTask, getErr := tasks.GetByID(ctx, tenant, task.ID)
		require.NoError(t, getErr)
		assert.Equal(t, domain.TaskStatusReady, storedTask.Status)
		assert.Equal(t, task.RecordVersion, storedTask.RecordVersion)
		storedInstance, getErr := instances.GetByID(ctx, tenant, inst.ID)
		require.NoError(t, getErr)
		assert.Equal(t, domain.InstanceStatusRunning, storedInstance.Status, "the earlier write of the same transaction was rolled back")
		assert.Equal(t, inst.RecordVersion, storedInstance.RecordVersion)
	})
}
