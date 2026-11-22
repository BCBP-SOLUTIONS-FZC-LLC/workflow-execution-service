//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestInstanceRepo_ListByTenant_RowsWithTheSameCreatedAtPageInIDOrderWithoutGapsOrRepeats(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	tenant, otherTenant := uuid.New(), uuid.New()
	tied := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	seed := func(tenantID uuid.UUID, createdAt time.Time) uuid.UUID {
		inst := newInstance(tenantID, createdAt)
		require.NoError(t, repo.Create(ctx, inst))
		seedInstanceAt(t, superPool, ctx, inst.ID, createdAt)
		return inst.ID
	}
	newest := seed(tenant, tied.Add(time.Minute))
	tiedIDs := make([]uuid.UUID, 0, 7)
	for i := 0; i < 7; i++ {
		tiedIDs = append(tiedIDs, seed(tenant, tied))
	}
	oldest := seed(tenant, tied.Add(-time.Minute))
	otherTenantRow := seed(otherTenant, tied.Add(-time.Second))
	sort.Slice(tiedIDs, func(i, j int) bool { return bytes.Compare(tiedIDs[i][:], tiedIDs[j][:]) > 0 })
	want := append(append([]uuid.UUID{newest}, tiedIDs...), oldest)

	var got []uuid.UUID
	var pageSizes []int
	var cursor *port.Cursor
	for {
		page, next, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{After: cursor, Limit: 2})
		require.NoError(t, err)
		pageSizes = append(pageSizes, len(page))
		for _, inst := range page {
			got = append(got, inst.ID)
		}
		if next == nil {
			break
		}
		cursor = next
		require.LessOrEqual(t, len(pageSizes), 10, "paging must end")
	}

	assert.Equal(t, want, got, "newest first, then the tied rows by descending id, then the oldest; every row once")
	assert.Equal(t, []int{2, 2, 2, 2, 1}, pageSizes, "page boundaries fall inside the group of tied rows")

	t.Run("a cursor taken inside the tie group returns only rows strictly after it", func(t *testing.T) {
		first, next, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{Limit: 3})
		require.NoError(t, err)
		require.Equal(t, want[:3], []uuid.UUID{first[0].ID, first[1].ID, first[2].ID})
		require.NotNil(t, next)
		assert.Equal(t, want[2], next.ID)

		rest, _, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{After: next, Limit: 50})

		require.NoError(t, err)
		var restIDs []uuid.UUID
		for _, inst := range rest {
			restIDs = append(restIDs, inst.ID)
		}
		assert.Equal(t, want[3:], restIDs, "the row the cursor points at is not repeated and no tied row is skipped")
	})

	t.Run("the same cursor under another tenant's id lists none of this tenant's rows", func(t *testing.T) {
		_, next, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{Limit: 3})
		require.NoError(t, err)
		require.NotNil(t, next)

		rows, _, err := repo.ListByTenant(ctx, otherTenant, port.InstanceListFilter{}, port.PageRequest{After: next, Limit: 50})

		require.NoError(t, err)
		require.Len(t, rows, 1, "the other tenant's own row is older than the cursor; none of this tenant's nine rows show")
		assert.Equal(t, otherTenantRow, rows[0].ID)
		assert.Equal(t, otherTenant, rows[0].TenantID)
	})
}
