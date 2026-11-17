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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestRecencyGuardRepo_ShouldApplyUnlessOlder_TieApplies_StrictlyOlderSkips(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	ctx := context.Background()
	scopeKey := "tenant:" + uuid.New().String()
	at := time.Now().Truncate(time.Second)

	should, err := repo.ShouldApplyUnlessOlder(ctx, scopeKey, at)
	require.NoError(t, err)
	assert.True(t, should, "no prior value applies")

	require.NoError(t, repo.Commit(ctx, scopeKey, at))

	should, err = repo.ShouldApplyUnlessOlder(ctx, scopeKey, at)
	require.NoError(t, err)
	assert.True(t, should, "an equal timestamp applies")
	should, err = repo.ShouldApplyUnlessOlder(ctx, scopeKey, at.Add(-time.Microsecond))
	require.NoError(t, err)
	assert.False(t, should, "a strictly older timestamp is skipped")
	should, err = repo.ShouldApplyUnlessOlder(ctx, scopeKey, at.Add(time.Second))
	require.NoError(t, err)
	assert.True(t, should)

	should, err = repo.ShouldApply(ctx, scopeKey, at)
	require.NoError(t, err)
	assert.False(t, should, "the ordinary check still skips a tie")
}
