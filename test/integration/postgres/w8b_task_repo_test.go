//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTaskRepo_GetByID_InsideATransactionOpenedForAnotherTenant_ReadsThatTenantsRows(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	cases := []struct {
		name string
		run  func(t *testing.T, tenantA, tenantB uuid.UUID, taskOfA *domain.Task)
	}{
		{
			name: "outside a transaction the repo's own tenant applies",
			run: func(t *testing.T, tenantA, tenantB uuid.UUID, taskOfA *domain.Task) {
				got, err := tasks.GetByID(ctx, tenantB, taskOfA.ID)
				require.ErrorIs(t, err, domain.ErrNotFound)
				assert.Nil(t, got)
			},
		},
		{
			name: "inside a transaction the transaction's tenant applies, not the argument",
			run: func(t *testing.T, tenantA, tenantB uuid.UUID, taskOfA *domain.Task) {
				var got *domain.Task
				err := transactor.RunInTx(withGUC(ctx, tenantA), func(txCtx context.Context) error {
					var getErr error
					got, getErr = tasks.GetByID(txCtx, tenantB, taskOfA.ID)
					return getErr
				})
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, taskOfA.ID, got.ID)
				assert.Equal(t, tenantA, got.TenantID)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantA, tenantB := uuid.New(), uuid.New()
			taskOfA := w8bSeedTask(t, appPool, tenantA)
			tc.run(t, tenantA, tenantB, taskOfA)
		})
	}
}

func TestTaskRepo_ATransactionContextUsedAfterTheTransactionEnded_FailsOnTheClosedTransaction(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	cases := []struct {
		name string
		end  func(txCtx context.Context) error
	}{
		{name: "after a commit", end: func(context.Context) error { return nil }},
		{name: "after a rollback", end: func(context.Context) error { return assert.AnError }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			task := w8bSeedTask(t, appPool, tenantID)
			var captured context.Context
			_ = transactor.RunInTx(withGUC(ctx, tenantID), func(txCtx context.Context) error {
				captured = txCtx
				return tc.end(txCtx)
			})
			require.NotNil(t, captured)

			got, err := tasks.GetByID(captured, tenantID, task.ID)

			require.ErrorIs(t, err, pgx.ErrTxClosed)
			assert.Nil(t, got, "the read did not fall back to a pooled connection")
		})
	}
}

func w8bBlockedLockWaiters(superPool *pgcommon.Pool) (int, error) {
	var waiting int
	err := superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype IN ('transactionid', 'tuple')`).Scan(&waiting)
	})
	return waiting, err
}

func TestTaskRepo_UpdateStatus_TwoDefersWithTheSameRecordVersion_OneWinsAndTheOtherConflicts(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantID := uuid.New()
	task := w8bSeedTask(t, appPool, tenantID)

	held := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWinner := func() { releaseOnce.Do(func() { close(release) }) }
	winner := make(chan error, 1)
	go func() {
		winner <- transactor.RunInTx(withGUC(ctx, tenantID), func(txCtx context.Context) error {
			if _, err := tasks.UpdateStatus(txCtx, tenantID, task.ID, domain.TaskStatusDeferred, task.RecordVersion); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	t.Cleanup(releaseWinner)
	select {
	case <-held:
	case err := <-winner:
		require.FailNow(t, "the first defer ended before holding the row", "error: %v", err)
	}

	type outcome struct {
		task *domain.Task
		err  error
	}
	loser := make(chan outcome, 1)
	go func() {
		got, err := tasks.UpdateStatus(ctx, tenantID, task.ID, domain.TaskStatusDeferred, task.RecordVersion)
		loser <- outcome{task: got, err: err}
	}()
	require.Eventually(t, func() bool {
		waiting, err := w8bBlockedLockWaiters(superPool)
		return err == nil && waiting > 0
	}, 30*time.Second, 20*time.Millisecond, "the second defer waits for the first one's row lock")
	releaseWinner()

	require.NoError(t, <-winner)
	second := <-loser
	require.ErrorIs(t, second.err, domain.ErrRecordVersionConflict)
	assert.Nil(t, second.task)

	stored, err := tasks.GetByID(ctx, tenantID, task.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusDeferred, stored.Status)
	assert.Equal(t, task.RecordVersion+1, stored.RecordVersion, "the version moved once, for the winner only")
}

func TestTaskRepo_BumpRecordVersion_WhenTheStatementFails_ReturnsTheErrorAndChangesNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	tasks := postgres.NewTaskRepo(appPool)

	cases := []struct {
		name    string
		derive  func(parent context.Context) (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "cancelled context",
			derive: func(parent context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(parent)
				cancel()
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "expired deadline",
			derive: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(parent, time.Now().Add(-time.Hour))
			},
			wantErr: context.DeadlineExceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			task := w8bSeedTask(t, appPool, tenantID)

			var bumped *domain.Task
			err := transactor.RunInTx(withGUC(context.Background(), tenantID), func(txCtx context.Context) error {
				failing, cancel := tc.derive(txCtx)
				defer cancel()
				// The transaction is already open, so the failure comes from the statement itself.
				var bumpErr error
				bumped, bumpErr = tasks.BumpRecordVersion(failing, tenantID, task.ID, task.RecordVersion)
				return bumpErr
			})

			require.ErrorIs(t, err, tc.wantErr)
			for _, answer := range []error{domain.ErrNotFound, domain.ErrRecordVersionConflict} {
				assert.NotErrorIs(t, err, answer, "a database failure must not look like a client answer")
			}
			assert.Nil(t, bumped)
			stored, getErr := tasks.GetByID(context.Background(), tenantID, task.ID)
			require.NoError(t, getErr)
			assert.Equal(t, task.RecordVersion, stored.RecordVersion)
		})
	}
}

func TestTaskRepo_UpdateStatus_ATaskChangedSinceItWasListed_RollsBackTheWholeTransactionAndARerunSucceeds(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, postgres.NewInstanceRepo(appPool).Create(ctx, inst))
	first, second := newTask(tenantID, inst.ID), newTask(tenantID, inst.ID)
	require.NoError(t, tasks.Create(ctx, first))
	require.NoError(t, tasks.Create(ctx, second))

	listed, _, err := tasks.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, listed, 2)
	_, err = tasks.BumpRecordVersion(ctx, tenantID, second.ID, second.RecordVersion)
	require.NoError(t, err, "someone else changes the second task after the list was read")

	// The task that is still current goes first, so a missing rollback would leave its write behind.
	failInOrder := func(txCtx context.Context, read []*domain.Task) error {
		byID := map[uuid.UUID]*domain.Task{}
		for _, task := range read {
			byID[task.ID] = task
		}
		for _, id := range []uuid.UUID{first.ID, second.ID} {
			task := byID[id]
			if _, err := tasks.UpdateStatus(txCtx, tenantID, task.ID, domain.TaskStatusFailed, task.RecordVersion); err != nil {
				return err
			}
		}
		return nil
	}

	err = transactor.RunInTx(withGUC(ctx, tenantID), func(txCtx context.Context) error { return failInOrder(txCtx, listed) })

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	for _, id := range []uuid.UUID{first.ID, second.ID} {
		stored, getErr := tasks.GetByID(ctx, tenantID, id)
		require.NoError(t, getErr)
		assert.Equal(t, domain.TaskStatusReady, stored.Status, "the failed transaction left no task failed")
	}

	fresh, _, err := tasks.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.NoError(t, transactor.RunInTx(withGUC(ctx, tenantID), func(txCtx context.Context) error { return failInOrder(txCtx, fresh) }))
	for _, id := range []uuid.UUID{first.ID, second.ID} {
		stored, getErr := tasks.GetByID(ctx, tenantID, id)
		require.NoError(t, getErr)
		assert.Equal(t, domain.TaskStatusFailed, stored.Status, "the rerun with the current versions fails both")
	}
}
