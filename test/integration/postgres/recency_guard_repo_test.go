//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
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

func TestRecencyGuardRepo_ShouldApply_NoPriorRow_ReturnsTrue(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)

	should, err := repo.ShouldApply(context.Background(), "tenant:"+uuid.New().String(), time.Now())
	require.NoError(t, err)
	assert.True(t, should)
}

func TestRecencyGuardRepo_ShouldApply_TieResolvesToSkip(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "tenant:" + uuid.New().String()
	at := time.Now().Truncate(time.Second)

	require.NoError(t, repo.Commit(ctx, scopeKey, at))

	should, err := repo.ShouldApply(ctx, scopeKey, at)
	require.NoError(t, err)
	assert.False(t, should, "an equal timestamp must resolve to skip, not apply")
}

func TestRecencyGuardRepo_ShouldApply_OlderSkip_NewerApplies(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "tenant:" + uuid.New().String()
	base := time.Now().Truncate(time.Second)

	require.NoError(t, repo.Commit(ctx, scopeKey, base))

	should, err := repo.ShouldApply(ctx, scopeKey, base.Add(-time.Minute))
	require.NoError(t, err)
	assert.False(t, should)

	should, err = repo.ShouldApply(ctx, scopeKey, base.Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, should)
}

func TestRecencyGuardRepo_CheckAndCommit_AtomicSingleShot(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "user_availability:" + uuid.New().String()
	base := time.Now().Truncate(time.Second)

	applied, err := repo.CheckAndCommit(ctx, scopeKey, base)
	require.NoError(t, err)
	assert.True(t, applied, "the first commit for a scope_key must apply")

	applied, err = repo.CheckAndCommit(ctx, scopeKey, base.Add(-time.Minute))
	require.NoError(t, err)
	assert.False(t, applied, "an older event must not apply")

	applied, err = repo.CheckAndCommit(ctx, scopeKey, base.Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, applied, "a strictly newer event must apply")
}

func TestRecencyGuardRepo_CheckAndCommit_ConcurrentRace(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "template:" + uuid.New().String()
	base := time.Now().Truncate(time.Second)

	older := base
	newer := base.Add(time.Hour)

	var wg sync.WaitGroup
	results := make([]bool, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		applied, err := repo.CheckAndCommit(ctx, scopeKey, older)
		require.NoError(t, err)
		results[0] = applied
	}()
	go func() {
		defer wg.Done()
		applied, err := repo.CheckAndCommit(ctx, scopeKey, newer)
		require.NoError(t, err)
		results[1] = applied
	}()
	wg.Wait()

	should, err := repo.ShouldApply(ctx, scopeKey, older)
	require.NoError(t, err)
	assert.False(t, should, "after the race, the stored value must be the strictly newer one")
}

func TestRecencyGuardRepo_WithLock_RunsFnAndPropagatesResult(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "tenant:" + uuid.New().String()

	var ran bool
	err := repo.WithLock(ctx, scopeKey, func(context.Context) error {
		ran = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, ran)

	sentinel := errors.New("fn failed")
	err = repo.WithLock(ctx, scopeKey, func(context.Context) error {
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel, "WithLock must propagate fn's own error")
}

// TestRecencyGuardRepo_WithLock_SerializesSameScopeKey is the regression test
// for the race WithLock exists to close: two concurrent callers sharing a
// scopeKey must run their fn one at a time, never overlapping, while two
// callers on DIFFERENT scope keys must not block each other at all.
func TestRecencyGuardRepo_WithLock_SerializesSameScopeKey(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "tenant:" + uuid.New().String()

	var mu sync.Mutex
	inside := 0
	maxConcurrent := 0
	track := func() (release func()) {
		mu.Lock()
		inside++
		if inside > maxConcurrent {
			maxConcurrent = inside
		}
		mu.Unlock()
		return func() {
			mu.Lock()
			inside--
			mu.Unlock()
		}
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for range 3 {
		go func() {
			defer wg.Done()
			err := repo.WithLock(ctx, scopeKey, func(context.Context) error {
				release := track()
				time.Sleep(50 * time.Millisecond)
				release()
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, maxConcurrent, "callers sharing a scope key must never run fn concurrently")
}

func TestRecencyGuardRepo_WithLock_DifferentScopeKeysDoNotBlock(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()

	release := make(chan struct{})
	started := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := repo.WithLock(ctx, "tenant:"+uuid.New().String(), func(context.Context) error {
			close(started)
			<-release
			return nil
		})
		assert.NoError(t, err)
	}()

	<-started
	// A different scope key must acquire immediately, not wait for the
	// first goroutine's lock (still held) to release.
	done := make(chan struct{})
	go func() {
		err := repo.WithLock(ctx, "tenant:"+uuid.New().String(), func(context.Context) error {
			return nil
		})
		assert.NoError(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a different scope key must not be blocked by another key's held lock")
	}

	close(release)
	wg.Wait()
}

func TestRecencyGuardRepo_Commit_IsMonotonic(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "tenant:" + uuid.New().String()
	newer := time.Now().Truncate(time.Second)
	older := newer.Add(-time.Hour)

	require.NoError(t, repo.Commit(ctx, scopeKey, newer))
	require.NoError(t, repo.Commit(ctx, scopeKey, older))

	should, err := repo.ShouldApply(ctx, scopeKey, older)
	require.NoError(t, err)
	assert.False(t, should, "committing an older value after a newer one must not lower the stored value")
}

func TestRecencyGuardRepo_WithLock_WaitersForOneScopeDoNotStarveOtherScopes(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)
	lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn, MaxConns: 2})
	require.NoError(t, err)
	t.Cleanup(lockPool.Close)
	repo := postgres.NewRecencyGuardRepo(lockPool, lockPool)
	ctx := context.Background()
	busy, other := "tenant:"+uuid.NewString(), "tenant:"+uuid.NewString()

	holding := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = repo.WithLock(ctx, busy, func(context.Context) error {
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = repo.WithLock(waitCtx, busy, func(context.Context) error { return nil })
		}()
	}
	time.Sleep(200 * time.Millisecond)

	otherCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = repo.WithLock(otherCtx, other, func(context.Context) error { return nil })
	close(release)
	wg.Wait()

	require.NoError(t, err, "an event for another tenant must not wait behind three queued events for a busy one, whatever the lock pool size")
}

func TestRecencyGuardRepo_WithLock_ALockConnectionThatDiesLeavesNoLockAndIsNotReused(t *testing.T) {
	superPool, dsn := fixtures.NewTestPoolAndDSN(t)
	lockPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{DSN: dsn, MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(lockPool.Close)
	repo := postgres.NewRecencyGuardRepo(lockPool, lockPool)
	ctx := context.Background()
	key := "tenant:" + uuid.NewString()

	err = repo.WithLock(ctx, key, func(context.Context) error {
		return superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()`)
			return err
		})
	})
	require.NoError(t, err, "the handler's own result is returned; a failed release is not its error")

	again, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, repo.WithLock(again, key, func(context.Context) error { return nil }), "the next call must get a working connection")
}
