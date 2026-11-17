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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTenantPurge_DeletesTheTenantsEndedDelegationsAndLeavesAnotherTenantsAlone(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	ended := postgres.NewEndedDelegationRepo(superPool)
	ctx := context.Background()
	due, other := uuid.New(), uuid.New()
	for i := 0; i < 7; i++ {
		require.NoError(t, ended.MarkEnded(ctx, due, uuid.New()))
	}
	require.NoError(t, ended.MarkEnded(ctx, other, uuid.New()))
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	require.NoError(t, repo.RecordOffboarded(ctx, due, now.Add(-48*time.Hour), now.Add(-time.Hour)))

	has, err := repo.HasRows(ctx, due)
	require.NoError(t, err)
	assert.True(t, has, "an ended delegation alone counts as a row of the tenant")

	recorded := map[string]int64{}
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: repo, Data: repo, Batch: 3, Interval: time.Hour,
		Now:    func() time.Time { return now },
		Record: func(table string, n int64) { recorded[table] += n },
		Failed: func() {},
	})
	require.NoError(t, purger.Tick(ctx))

	assert.Zero(t, countRows(t, superPool, "ended_delegation", due), "the purged tenant keeps no ended delegations")
	assert.Equal(t, 1, countRows(t, superPool, "ended_delegation", other), "another tenant's ended delegations stay")
	assert.EqualValues(t, 7, recorded["ended_delegation"])
	has, err = repo.HasRows(ctx, due)
	require.NoError(t, err)
	assert.False(t, has)
}
