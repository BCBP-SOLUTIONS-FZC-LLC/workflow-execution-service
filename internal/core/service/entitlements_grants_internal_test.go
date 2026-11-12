package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingReader struct{ calls map[uuid.UUID]int }

func (r *countingReader) FeatureFlags(_ context.Context, tenantID uuid.UUID) ([]string, error) {
	r.calls[tenantID]++
	return []string{"dedicated_workflow_queue"}, nil
}

func newCapGrants(t *testing.T, now *time.Time) (*EntitlementsGrants, *countingReader) {
	t.Helper()
	reader := &countingReader{calls: map[uuid.UUID]int{}}
	g, err := NewEntitlementsGrants(EntitlementsGrantsDeps{
		Reader: reader, TTL: time.Minute, FreshnessBound: 10 * time.Minute, Now: func() time.Time { return *now },
	})
	require.NoError(t, err)
	return g, reader
}

func TestEntitlementsGrants_TheCacheHoldsAtMostTenThousandTenantsAndDropsTheOldestFirst(t *testing.T) {
	now := time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	grants, reader := newCapGrants(t, &now)
	ctx := context.Background()
	require.Equal(t, 10_000, entitlementsCacheCap, "the cap is a documented number, not just a constant")
	tenants := make([]uuid.UUID, entitlementsCacheCap+3)
	for i := range tenants {
		tenants[i] = uuid.New()
		_, err := grants.HasFeature(ctx, tenants[i], "dedicated_workflow_queue")
		require.NoError(t, err)
		now = now.Add(time.Millisecond)
	}

	assert.Len(t, grants.entries, 10_000)
	newest := tenants[len(tenants)-1]
	_, err := grants.HasFeature(ctx, newest, "dedicated_workflow_queue")
	require.NoError(t, err)
	assert.Equal(t, 1, reader.calls[newest], "the newest tenant is kept")
	_, err = grants.HasFeature(ctx, tenants[0], "dedicated_workflow_queue")
	require.NoError(t, err)
	assert.Equal(t, 2, reader.calls[tenants[0]], "the oldest tenant was dropped and is read again")
}

func TestEntitlementsGrants_AFullCacheDropsEveryExpiredEntryBeforeAnyLiveOne(t *testing.T) {
	now := time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	grants, reader := newCapGrants(t, &now)
	ctx := context.Background()
	old := make([]uuid.UUID, entitlementsCacheCap-1)
	for i := range old {
		old[i] = uuid.New()
		_, err := grants.HasFeature(ctx, old[i], "dedicated_workflow_queue")
		require.NoError(t, err)
	}
	now = now.Add(10 * time.Minute)
	live := uuid.New()
	_, err := grants.HasFeature(ctx, live, "dedicated_workflow_queue")
	require.NoError(t, err)
	require.Len(t, grants.entries, entitlementsCacheCap)
	now = now.Add(time.Second)

	_, err = grants.HasFeature(ctx, uuid.New(), "dedicated_workflow_queue")
	require.NoError(t, err)

	assert.Len(t, grants.entries, 2, "the entries past the bound all went, the live one stayed")
	_, err = grants.HasFeature(ctx, live, "dedicated_workflow_queue")
	require.NoError(t, err)
	assert.Equal(t, 1, reader.calls[live])
}

func TestEntitlementsGrants_AnEntryOlderThanItsTTLButWithinTheBoundSurvivesTheSweep(t *testing.T) {
	now := time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	grants, _ := newCapGrants(t, &now)
	ctx := context.Background()
	held := make([]uuid.UUID, entitlementsCacheCap)
	for i := range held {
		held[i] = uuid.New()
		_, err := grants.HasFeature(ctx, held[i], "dedicated_workflow_queue")
		require.NoError(t, err)
	}
	now = now.Add(5 * time.Minute)

	_, err := grants.HasFeature(ctx, uuid.New(), "dedicated_workflow_queue")
	require.NoError(t, err)

	assert.Len(t, grants.entries, 10_000, "the full cache dropped one entry (the oldest), not every entry past the TTL")
	survivors := 0
	for _, id := range held {
		if _, ok := grants.entries[id]; ok {
			survivors++
		}
	}
	assert.Equal(t, 9_999, survivors)
}
