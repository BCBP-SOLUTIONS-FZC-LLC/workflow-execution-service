//go:build integration

package postgres_test

import (
	"context"
	"errors"
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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w24bRetentionRow struct {
	OffboardedAt *time.Time
	ReturnedAt   *time.Time
	PurgedAt     *time.Time
	Attempts     int
	Version      int64
}

func w24bReadRetention(t *testing.T, pool *pgcommon.Pool, tenant uuid.UUID) (w24bRetentionRow, bool) {
	t.Helper()
	var row w24bRetentionRow
	found := true
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		err := conn.QueryRow(ctx,
			`SELECT offboarded_at, returned_at, purged_at, attempts, record_version FROM tenant_retention WHERE tenant_id = $1`, tenant,
		).Scan(&row.OffboardedAt, &row.ReturnedAt, &row.PurgedAt, &row.Attempts, &row.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			found = false
			return nil
		}
		return err
	}))
	return row, found
}

func w24bDueTenants(t *testing.T, repo *postgres.TenantRetentionRepo, now time.Time, limit int) []uuid.UUID {
	t.Helper()
	due, err := repo.ListDue(context.Background(), now, limit)
	require.NoError(t, err)
	var ids []uuid.UUID
	for _, d := range due {
		ids = append(ids, d.TenantID)
	}
	return ids
}

func TestTenantRetentionRepo_RecordReturned_BumpsTheVersionOnlyWhenTheReturnChangesTheRow(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewTenantRetentionRepo(pool)
	ctx := context.Background()
	offboarded := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	t.Run("a tenant never seen before gets a row holding only the return", func(t *testing.T) {
		tenant := uuid.New()
		require.NoError(t, repo.RecordReturned(ctx, tenant, offboarded))

		row, found := w24bReadRetention(t, pool, tenant)
		require.True(t, found)
		require.NotNil(t, row.ReturnedAt)
		assert.True(t, row.ReturnedAt.Equal(offboarded))
		assert.Nil(t, row.OffboardedAt)
		assert.Nil(t, row.PurgedAt)
		assert.Equal(t, int64(1), row.Version)
		assert.NotContains(t, w24bDueTenants(t, repo, offboarded.Add(1000*time.Hour), 100), tenant, "a return alone is never due for purge")
	})

	t.Run("a known tenant takes only a later return, one version step each time", func(t *testing.T) {
		tenant := uuid.New()
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, offboarded, offboarded.Add(time.Hour)))

		steps := []struct {
			name        string
			returnAt    time.Time
			wantReturn  *time.Time
			wantVersion int64
		}{
			{name: "a return at the offboarding instant is ignored", returnAt: offboarded, wantReturn: nil, wantVersion: 1},
			{name: "a return before the offboarding is ignored", returnAt: offboarded.Add(-time.Minute), wantReturn: nil, wantVersion: 1},
			{name: "a later return is recorded", returnAt: offboarded.Add(time.Hour), wantReturn: ptrTime(offboarded.Add(time.Hour)), wantVersion: 2},
			{name: "the same return again is ignored", returnAt: offboarded.Add(time.Hour), wantReturn: ptrTime(offboarded.Add(time.Hour)), wantVersion: 2},
			{name: "an older return than the recorded one is ignored", returnAt: offboarded.Add(30 * time.Minute), wantReturn: ptrTime(offboarded.Add(time.Hour)), wantVersion: 2},
			{name: "a still later return replaces the recorded one", returnAt: offboarded.Add(2 * time.Hour), wantReturn: ptrTime(offboarded.Add(2 * time.Hour)), wantVersion: 3},
		}
		for _, step := range steps {
			require.NoError(t, repo.RecordReturned(ctx, tenant, step.returnAt), step.name)
			row, found := w24bReadRetention(t, pool, tenant)
			require.True(t, found, step.name)
			if step.wantReturn == nil {
				assert.Nil(t, row.ReturnedAt, step.name)
			} else {
				require.NotNil(t, row.ReturnedAt, step.name)
				assert.True(t, row.ReturnedAt.Equal(*step.wantReturn), "%s: got %s", step.name, row.ReturnedAt)
			}
			assert.Equal(t, step.wantVersion, row.Version, step.name)
			require.NotNil(t, row.OffboardedAt, step.name)
			assert.True(t, row.OffboardedAt.Equal(offboarded), "the offboarding date is never touched by a return: %s", step.name)
		}
	})
}

func TestTenantRetentionRepo_ListDue_SelectsByDateAndOrdersByPurgeDateThenTenant(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewTenantRetentionRepo(pool)
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

	early := uuid.New()
	tieA, tieB := uuid.New(), uuid.New()
	atNow := uuid.New()
	justAfterNow := uuid.New()
	waiting := uuid.New()
	waitingUntilNow := uuid.New()
	for tenant, purgeAfter := range map[uuid.UUID]time.Time{
		early:           now.Add(-48 * time.Hour),
		tieA:            now.Add(-time.Hour),
		tieB:            now.Add(-time.Hour),
		atNow:           now,
		justAfterNow:    now.Add(time.Microsecond),
		waiting:         now.Add(-72 * time.Hour),
		waitingUntilNow: now.Add(-30 * time.Minute),
	} {
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, purgeAfter.Add(-time.Hour), purgeAfter))
	}
	require.NoError(t, repo.RecordFailure(ctx, waiting, 1, 1, now.Add(time.Microsecond)))
	require.NoError(t, repo.RecordFailure(ctx, waitingUntilNow, 1, 1, now))

	t.Run("ordered by purge date then tenant id, boundaries included", func(t *testing.T) {
		first, second := tieA, tieB
		if second.String() < first.String() {
			first, second = second, first
		}
		assert.Equal(t, []uuid.UUID{early, first, second, waitingUntilNow, atNow}, w24bDueTenants(t, repo, now, 100),
			"a purge date or retry time equal to now is due; one microsecond later is not; a tenant waiting on a retry is skipped until then")
	})

	t.Run("limit 0 takes the default page, the same as the other list repos", func(t *testing.T) {
		assert.Len(t, w24bDueTenants(t, repo, now, 0), 5)
	})

	t.Run("limit 1 returns the first in order", func(t *testing.T) {
		assert.Equal(t, []uuid.UUID{early}, w24bDueTenants(t, repo, now, 1))
	})
}

func TestTenantRetentionRepo_MarkPurgedAndRecordFailure_RefuseAStaleVersionAndAcceptTheCurrentOne(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewTenantRetentionRepo(pool)
	ctx := context.Background()
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	later := at.Add(time.Hour)

	t.Run("a tenant with no row is a changed record", func(t *testing.T) {
		_, err := repo.MarkPurged(ctx, uuid.New(), 1, later)
		require.ErrorIs(t, err, port.ErrTenantRetentionChanged)
		require.ErrorIs(t, repo.RecordFailure(ctx, uuid.New(), 1, 1, later), port.ErrTenantRetentionChanged)
	})

	t.Run("a stale version is refused and leaves the row as it was", func(t *testing.T) {
		tenant := uuid.New()
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, at, at))
		require.NoError(t, repo.RecordFailure(ctx, tenant, 1, 1, at))

		_, err := repo.MarkPurged(ctx, tenant, 1, later)
		require.ErrorIs(t, err, port.ErrTenantRetentionChanged)

		row, _ := w24bReadRetention(t, pool, tenant)
		assert.Nil(t, row.PurgedAt)
		assert.Equal(t, int64(2), row.Version)
		assert.Equal(t, 1, row.Attempts)
	})

	t.Run("the matching version is accepted and the next version is returned", func(t *testing.T) {
		tenant := uuid.New()
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, at, at))

		next, err := repo.MarkPurged(ctx, tenant, 1, later)
		require.NoError(t, err)
		assert.Equal(t, int64(2), next)

		row, _ := w24bReadRetention(t, pool, tenant)
		require.NotNil(t, row.PurgedAt)
		assert.True(t, row.PurgedAt.Equal(later))
		assert.Zero(t, row.Attempts)
		assert.NotContains(t, w24bDueTenants(t, repo, later.Add(time.Hour), 100), tenant, "a purged tenant is not due again")

		_, err = repo.MarkPurged(ctx, tenant, 1, later)
		require.ErrorIs(t, err, port.ErrTenantRetentionChanged, "the version it was read at is now stale")
	})

	t.Run("a version read before the tenant returned is stale for both writes", func(t *testing.T) {
		tenant := uuid.New()
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, at, at))
		due := w24bDueTenants(t, repo, later, 100)
		require.Contains(t, due, tenant)
		require.NoError(t, repo.RecordReturned(ctx, tenant, later))

		_, err := repo.MarkPurged(ctx, tenant, 1, later)
		require.ErrorIs(t, err, port.ErrTenantRetentionChanged)
		require.ErrorIs(t, repo.RecordFailure(ctx, tenant, 1, 1, later), port.ErrTenantRetentionChanged)

		row, _ := w24bReadRetention(t, pool, tenant)
		assert.Nil(t, row.PurgedAt, "a tenant that returned is not marked purged")
		assert.Zero(t, row.Attempts)
		assert.Equal(t, int64(2), row.Version)
	})
}

func TestTenantRetentionRepo_PurgeBatch_DeletesOneTableAtATimeInTheFixedOrder(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)

	tenant := uuid.New()
	seedPurgeTenant(t, superPool, tenant, 1, 2)
	require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-48*time.Hour), now.Add(-time.Hour)))

	want := []map[string]int64{
		{"assignee_overrides": 1},
		{"workflow_task_assignment": 2},
		{"workflow_task_link": 1},
		{"workflow_task": 2},
		{"workflow_instance": 1},
		{"connector_documents": 1},
		{"connector_document_attempts": 1},
		{"connector_send_intents": 1},
		{"user_availability": 1},
		{"active_task_queues": 1},
		{"event_recency_guard": 2},
	}
	var got []map[string]int64
	for i := 0; i < len(want)+1; i++ {
		deleted, err := repo.PurgeBatch(ctx, tenant, 1, 10)
		require.NoError(t, err)
		require.NotNil(t, deleted)
		if len(deleted) == 0 {
			break
		}
		got = append(got, deleted)
	}
	assert.Equal(t, want, got, "each call empties the first table that still has rows and reports only that table")

	deleted, err := repo.PurgeBatch(ctx, tenant, 1, 10)
	require.NoError(t, err)
	assert.NotNil(t, deleted)
	assert.Empty(t, deleted, "with nothing left the answer is an empty map")
	for _, table := range purgeTables {
		assert.Zero(t, countRows(t, superPool, table, tenant), table)
	}
}

func TestTenantRetentionRepo_PurgeBatch_TheQueueStepRemovesEveryQueueWhateverTheLimit(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	queues := postgres.NewActiveTaskQueueRepo(superPool)
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)

	tenant, bystander := uuid.New(), uuid.New()
	for _, owner := range []uuid.UUID{tenant, tenant, tenant, bystander} {
		_, err := queues.Register(ctx, owner, "queue-"+uuid.NewString())
		require.NoError(t, err)
	}
	require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-48*time.Hour), now.Add(-time.Hour)))

	deleted, err := repo.PurgeBatch(ctx, tenant, 1, 1)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"active_task_queues": 3}, deleted, "a limit of 1 does not hold the queue step back")
	assert.Zero(t, countRows(t, superPool, "active_task_queues", tenant))
	assert.Equal(t, 1, countRows(t, superPool, "active_task_queues", bystander), "another tenant's queue stays")
}

func TestTenantRetentionRepo_PurgeBatch_RefusesAStaleVersionAndAReturnWaitsForARunningBatch(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)

	t.Run("a stale version without any return deletes nothing", func(t *testing.T) {
		tenant := uuid.New()
		seedPurgeTenant(t, superPool, tenant, 1, 1)
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-48*time.Hour), now.Add(-time.Hour)))
		require.NoError(t, repo.RecordFailure(ctx, tenant, 1, 1, now.Add(-time.Minute)))

		deleted, err := repo.PurgeBatch(ctx, tenant, 1, 10)
		require.ErrorIs(t, err, port.ErrTenantRetentionChanged)
		assert.ErrorContains(t, err, "purge batch")
		assert.Nil(t, deleted)
		for _, table := range purgeTables {
			assert.NotZero(t, countRows(t, superPool, table, tenant), "%s untouched", table)
		}
	})

	t.Run("a return recorded while a batch runs waits until the batch commits", func(t *testing.T) {
		tenant := uuid.New()
		seedPurgeTenant(t, superPool, tenant, 1, 1)
		require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-48*time.Hour), now.Add(-time.Hour)))

		// The batch's first delete waits on a row this transaction holds, which
		// keeps the batch open (and the retention row locked) on demand.
		blockerHeld := make(chan struct{})
		blockerRelease := make(chan struct{})
		var releaseOnce sync.Once
		releaseBlocker := func() { releaseOnce.Do(func() { close(blockerRelease) }) }
		t.Cleanup(releaseBlocker)
		blockerDone := make(chan error, 1)
		go func() {
			blockerDone <- superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
				tx, err := conn.Begin(ctx)
				if err != nil {
					return err
				}
				defer func() { _ = tx.Rollback(ctx) }()
				var held uuid.UUID
				if err := tx.QueryRow(ctx, `SELECT id FROM assignee_overrides WHERE tenant_id = $1 FOR UPDATE`, tenant).Scan(&held); err != nil {
					return err
				}
				close(blockerHeld)
				<-blockerRelease
				return tx.Rollback(ctx)
			})
		}()
		select {
		case <-blockerHeld:
		case err := <-blockerDone:
			require.FailNow(t, "the blocking transaction ended before holding the row", "error: %v", err)
		}

		type batchResult struct {
			deleted map[string]int64
			err     error
		}
		batchDone := make(chan batchResult, 1)
		go func() {
			deleted, batchErr := repo.PurgeBatch(ctx, tenant, 1, 10)
			batchDone <- batchResult{deleted, batchErr}
		}()
		require.Eventually(t, func() bool {
			waiting, waitErr := w8bBlockedLockWaiters(superPool)
			return waitErr == nil && waiting >= 1
		}, 30*time.Second, 20*time.Millisecond, "the batch waits on the held override row")

		returnDone := make(chan error, 1)
		go func() { returnDone <- repo.RecordReturned(ctx, tenant, now) }()
		require.Eventually(t, func() bool {
			waiting, waitErr := w8bBlockedLockWaiters(superPool)
			return waitErr == nil && waiting >= 2
		}, 30*time.Second, 20*time.Millisecond, "the return waits on the retention row the batch holds")
		select {
		case err := <-returnDone:
			require.FailNow(t, "the return finished while the batch was still running", "error: %v", err)
		default:
		}

		releaseBlocker()
		require.NoError(t, <-blockerDone)
		result := <-batchDone
		require.NoError(t, result.err)
		assert.Equal(t, map[string]int64{"assignee_overrides": 1}, result.deleted)
		require.NoError(t, <-returnDone)

		row, found := w24bReadRetention(t, superPool, tenant)
		require.True(t, found)
		require.NotNil(t, row.ReturnedAt)
		assert.Equal(t, int64(2), row.Version, "the return landed after the batch and moved the version")

		_, nextErr := repo.PurgeBatch(ctx, tenant, 1, 10)
		require.ErrorIs(t, nextErr, port.ErrTenantRetentionChanged, "the next batch sees the return")
		assert.NotZero(t, countRows(t, superPool, "workflow_instance", tenant))
	})
}
