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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestInstanceRepo_ListByTenant_StartedAfterIsInclusiveAndStartedBeforeIsExclusive(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()
	tenant := uuid.New()

	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	early := newInstance(tenant, base.Add(-time.Hour))
	boundary := newInstance(tenant, base)
	late := newInstance(tenant, base.Add(time.Hour))
	require.NoError(t, repo.Create(ctx, early))
	require.NoError(t, repo.Create(ctx, boundary))
	require.NoError(t, repo.Create(ctx, late))

	ids := func(filter port.InstanceListFilter) []uuid.UUID {
		items, _, err := repo.ListByTenant(ctx, tenant, filter, port.PageRequest{Limit: 10})
		require.NoError(t, err)
		out := make([]uuid.UUID, len(items))
		for i, item := range items {
			out[i] = item.ID
		}
		return out
	}

	assert.ElementsMatch(t, []uuid.UUID{boundary.ID, late.ID}, ids(port.InstanceListFilter{StartedAfter: &base}),
		"an instance started exactly at started_after is included")
	assert.ElementsMatch(t, []uuid.UUID{early.ID}, ids(port.InstanceListFilter{StartedBefore: &base}),
		"an instance started exactly at started_before is left out")
	assert.ElementsMatch(t, []uuid.UUID{boundary.ID}, ids(port.InstanceListFilter{StartedAfter: &base, StartedBefore: ptrTime(base.Add(time.Hour))}),
		"the two bounds together form a half-open window")
}

func ptrTime(t time.Time) *time.Time { return &t }
