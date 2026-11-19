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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// w24aScopeIsFree reports whether another session can take the scope's
// advisory lock; it gives the lock back at once.
func w24aScopeIsFree(t *testing.T, pool *pgcommon.Pool, scope string) bool {
	t.Helper()
	var free bool
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
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

// w24aHoldScope takes the scope's advisory lock on a session of pool and keeps
// it until the returned release function runs (or the test ends).
func w24aHoldScope(t *testing.T, pool *pgcommon.Pool, scope string) (release func()) {
	t.Helper()
	held := make(chan struct{})
	stop := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
			if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext($1))", scope); err != nil {
				return err
			}
			close(held)
			<-stop
			_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock(hashtext($1))", scope)
			return err
		})
	}()
	select {
	case <-held:
	case err := <-finished:
		require.FailNow(t, "the holder could not take the lock", "%v", err)
	}
	var once bool
	release = func() {
		if once {
			return
		}
		once = true
		close(stop)
		require.NoError(t, <-finished)
	}
	t.Cleanup(release)
	return release
}

// w24aWaitForLockAttempt blocks until a session other than the caller has run
// the repo's try-lock query, so a cancel afterwards ends a call that is
// already waiting for the lock.
func w24aWaitForLockAttempt(t *testing.T, pool *pgcommon.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		var attempts int
		err := pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity
				 WHERE pid <> pg_backend_pid() AND query LIKE 'SELECT pg_try_advisory_lock%'`).Scan(&attempts)
		})
		return err == nil && attempts > 0
	}, 30*time.Second, 20*time.Millisecond, "the waiting caller never tried the lock")
}

func TestRecencyGuardRepo_WithLock_ACallerWaitingOnAHeldScope_ReturnsTheWrappedContextErrorAndRunsNothing(t *testing.T) {
	superPool, dsn := fixtures.NewTestPoolAndDSN(t)
	lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(lockPool.Close)
	repo := postgres.NewRecencyGuardRepo(lockPool, lockPool)
	key := "tenant:" + uuid.NewString()
	release := w24aHoldScope(t, superPool, key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		result <- repo.WithLock(ctx, key, func(context.Context) error {
			ran <- struct{}{}
			return nil
		})
	}()
	w24aWaitForLockAttempt(t, superPool)
	cancel()

	var got error
	select {
	case got = <-result:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "WithLock did not return after its context was cancelled")
	}
	require.ErrorIs(t, got, context.Canceled)
	assert.ErrorContains(t, got, "recency guard: with lock")
	assert.Empty(t, ran, "the function never ran while another session held the scope")
	assert.False(t, w24aScopeIsFree(t, superPool, key), "the other session still holds its lock")

	release()
	require.True(t, w24aScopeIsFree(t, superPool, key), "the cancelled caller left no lock behind")
	again, cancelAgain := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelAgain()
	ranAfter := false
	require.NoError(t, repo.WithLock(again, key, func(context.Context) error {
		ranAfter = true
		return nil
	}))
	assert.True(t, ranAfter)
}

func TestRecencyGuardRepo_WithLock_AWaitOnOneTenantsScope_DoesNotBlockAnotherTenantsScope(t *testing.T) {
	superPool, dsn := fixtures.NewTestPoolAndDSN(t)
	lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(lockPool.Close)
	repo := postgres.NewRecencyGuardRepo(lockPool, lockPool)
	heldTenant, otherTenant := "tenant:"+uuid.NewString(), "tenant:"+uuid.NewString()
	w24aHoldScope(t, superPool, heldTenant)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- repo.WithLock(ctx, heldTenant, func(context.Context) error { return nil })
	}()

	w24aWaitForLockAttempt(t, superPool)
	otherCtx, otherCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer otherCancel()
	ranOther := false
	require.NoError(t, repo.WithLock(otherCtx, otherTenant, func(context.Context) error {
		ranOther = true
		return nil
	}), "a different tenant's scope is not blocked by the held one or its waiter")
	assert.True(t, ranOther)

	cancel()
	var got error
	select {
	case got = <-result:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "WithLock did not return after its deadline passed")
	}
	require.Error(t, got)
	assert.ErrorContains(t, got, "recency guard: with lock")
	assert.ErrorIs(t, got, context.Canceled)
}

func TestRecencyGuardRepo_WithLock_ReleasesTheLockAfterAnErrorOrACancel(t *testing.T) {
	superPool, dsn := fixtures.NewTestPoolAndDSN(t)
	lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(lockPool.Close)
	repo := postgres.NewRecencyGuardRepo(lockPool, lockPool)
	boom := errors.New("fn failed")

	cases := []struct {
		name string
		fn   func(cancel context.CancelFunc) func(ctx context.Context) error
		want error
	}{
		{
			name: "the function returns an error",
			fn: func(context.CancelFunc) func(context.Context) error {
				return func(context.Context) error { return boom }
			},
			want: boom,
		},
		{
			name: "the function cancels its own context and returns the cancellation",
			fn: func(cancel context.CancelFunc) func(context.Context) error {
				return func(ctx context.Context) error {
					cancel()
					<-ctx.Done()
					return ctx.Err()
				}
			},
			want: context.Canceled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "tenant:" + uuid.NewString()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := repo.WithLock(ctx, key, tc.fn(cancel))

			require.ErrorIs(t, err, tc.want)
			assert.ErrorContains(t, err, "recency guard: with lock")
			assert.True(t, w24aScopeIsFree(t, superPool, key), "no advisory lock outlives the call")
			again, cancelAgain := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelAgain()
			ran := false
			require.NoError(t, repo.WithLock(again, key, func(context.Context) error {
				ran = true
				return nil
			}))
			assert.True(t, ran, "the same scope can be locked again at once")
		})
	}
}

func TestRecencyGuardRepo_WithLock_WhenTheLockPoolIsClosed_FailsWrappedAppliesNothingAndTheDataPoolIsUntouched(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	dataPool := newAppRolePool(t, superPool, superDSN)
	lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: superDSN})
	require.NoError(t, err)
	lockPool.Close()
	repo := postgres.NewRecencyGuardRepo(dataPool, lockPool)
	ctx := context.Background()
	scope := "user-availability:" + uuid.NewString()
	eventTime := time.Now().UTC()
	ran := false

	err = repo.WithLock(ctx, scope, func(ctx context.Context) error {
		ran = true
		return repo.Commit(ctx, scope, eventTime)
	})

	require.ErrorContains(t, err, "recency guard: with lock")
	assert.True(t, pgcommon.IsPoolClosed(err), "the pool's own error stays reachable: %v", err)
	assert.False(t, ran)
	apply, checkErr := repo.ShouldApply(ctx, scope, eventTime)
	require.NoError(t, checkErr, "the data pool still works")
	assert.True(t, apply, "no recency row was written, so the event is still unapplied")
}
