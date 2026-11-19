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

func TestTenantRetentionRepo_ListDue_ANonPositiveLimitFallsBackToTheDefaultPageInsteadOfFailingOrReturningNothing(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewTenantRetentionRepo(pool)
	ctx := context.Background()
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	tenant := uuid.New()
	require.NoError(t, repo.RecordOffboarded(ctx, tenant, now.Add(-48*time.Hour), now.Add(-time.Hour)))

	for _, limit := range []int{-1, 0} {
		due, err := repo.ListDue(ctx, now, limit)
		require.NoError(t, err, "limit %d", limit)
		require.Len(t, due, 1, "limit %d", limit)
		assert.Equal(t, tenant, due[0].TenantID)
	}
}
