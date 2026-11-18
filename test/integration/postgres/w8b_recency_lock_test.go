//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// w8bScopeIsFree reports whether another session can take the scope's lock; it
// gives the lock back at once.
func w8bScopeIsFree(t *testing.T, superPool *pgcommon.Pool, scope string) bool {
	t.Helper()
	var free bool
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", scope).Scan(&free); err != nil {
			return err
		}
		if !free {
			return nil
		}
		var released bool
		return conn.QueryRow(ctx, "SELECT pg_advisory_unlock(hashtext($1))", scope).Scan(&released)
	}))
	return free
}

func TestRecencyGuardRepo_WithLocks_WhenLockingFails_ReleasesWhatWasTakenAndNeverRunsTheFunction(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)

	t.Run("the lock connection cannot be taken and the function never runs", func(t *testing.T) {
		lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: superDSN})
		require.NoError(t, err)
		lockPool.Close()
		repo := postgres.NewRecencyGuardRepo(appPool, lockPool)
		ran := false

		err = repo.WithLocks(context.Background(), []string{"w8b-pool-a", "w8b-pool-b"}, func(context.Context) error {
			ran = true
			return nil
		})

		require.ErrorContains(t, err, "recency guard: with lock: lock connection")
		assert.True(t, pgcommon.IsPoolClosed(err), "the pool's own error stays reachable: %v", err)
		assert.False(t, ran)
	})

	t.Run("a try-lock query fails after an earlier scope was taken", func(t *testing.T) {
		repo := postgres.NewRecencyGuardRepo(appPool, appPool)
		taken, failing := "w8b-taken-first", "w8b-unlockable\x00scope"
		ran := false

		err := repo.WithLocks(context.Background(), []string{failing, taken}, func(context.Context) error {
			ran = true
			return nil
		})

		require.ErrorContains(t, err, "recency guard: with lock: lock connection: acquire advisory lock")
		assert.False(t, ran)
		require.True(t, w8bScopeIsFree(t, superPool, taken), "the scope taken before the failure is released")

		followUp, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ranAfter := false
		require.NoError(t, repo.WithLocks(followUp, []string{taken}, func(context.Context) error {
			ranAfter = true
			return nil
		}), "the same scope can be locked again")
		assert.True(t, ranAfter)
	})
}
