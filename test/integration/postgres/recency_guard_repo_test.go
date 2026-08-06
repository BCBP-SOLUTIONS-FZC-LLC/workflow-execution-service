//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRecencyGuardRepo_ShouldApply_NoPriorRow_ReturnsTrue(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool)

	should, err := repo.ShouldApply(context.Background(), "tenant:"+uuid.New().String(), time.Now())
	require.NoError(t, err)
	assert.True(t, should)
}

func TestRecencyGuardRepo_ShouldApply_TieResolvesToSkip(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool)
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
	repo := postgres.NewRecencyGuardRepo(pool)
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
	repo := postgres.NewRecencyGuardRepo(pool)
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
	repo := postgres.NewRecencyGuardRepo(pool)
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

func TestRecencyGuardRepo_Commit_IsMonotonic(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool)
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
