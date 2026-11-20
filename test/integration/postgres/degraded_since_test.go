//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

type degradedSinceRig struct {
	t      *testing.T
	pool   *pgcommon.Pool
	repo   *postgres.InstanceRepo
	tenant uuid.UUID
	inst   *domain.Instance
}

func newDegradedSinceRig(t *testing.T) *degradedSinceRig {
	pool := fixtures.NewTestPool(t)
	rig := &degradedSinceRig{
		t: t, pool: pool, repo: postgres.NewInstanceRepo(pool), tenant: uuid.New(),
	}
	id := uuid.New()
	seedWorkflowInstance(t, pool, context.Background(), id, rig.tenant)
	inst, err := rig.repo.GetByID(context.Background(), rig.tenant, id)
	require.NoError(t, err)
	rig.inst = inst
	return rig
}

func (r *degradedSinceRig) setStatus(status domain.InstanceStatus) {
	r.t.Helper()
	inst, err := r.repo.UpdateStatus(
		context.Background(), r.tenant, r.inst.ID, status, r.inst.RecordVersion)
	require.NoError(r.t, err)
	r.inst = inst
}

func (r *degradedSinceRig) touchOtherColumn() {
	r.t.Helper()
	inst, err := r.repo.UpdateCurrentNodeKeys(
		context.Background(), r.tenant, r.inst.ID, []string{"later"}, r.inst.RecordVersion)
	require.NoError(r.t, err)
	r.inst = inst
}

func (r *degradedSinceRig) degradedSince() *time.Time {
	r.t.Helper()
	var since *time.Time
	require.NoError(r.t, r.pool.WithConn(context.Background(),
		func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx,
				`SELECT degraded_since FROM workflow_instance WHERE id = $1`, r.inst.ID).Scan(&since)
		}))
	return since
}

func (r *degradedSinceRig) backdateDegradedSince(age time.Duration) {
	r.t.Helper()
	require.NoError(r.t, r.pool.WithConn(context.Background(),
		func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx,
				`UPDATE workflow_instance SET degraded_since = now() - $2::interval WHERE id = $1`,
				r.inst.ID, age.String())
			return err
		}))
}

func TestOperationalStats_DegradedAgeKeepsGrowingAcrossLaterWrites(t *testing.T) {
	rig := newDegradedSinceRig(t)
	stats := postgres.NewOperationalStatsRepo(rig.pool)
	rig.setStatus(domain.InstanceStatusDegraded)
	rig.backdateDegradedSince(2 * time.Hour)

	rig.touchOtherColumn()
	got, err := stats.Read(context.Background())

	require.NoError(t, err)
	assert.InDelta(t, 7200, got.OldestDegradedAgeSeconds, 30,
		"a write to another column must not restart the stuck age")
}

func TestUpdateStatus_DegradedSinceFollowsTheStatus(t *testing.T) {
	rig := newDegradedSinceRig(t)
	require.Nil(t, rig.degradedSince(), "a running instance has no degraded moment")

	rig.setStatus(domain.InstanceStatusDegraded)
	entered := rig.degradedSince()
	require.NotNil(t, entered, "entering DEGRADED stamps the moment")

	rig.setStatus(domain.InstanceStatusDegraded)
	rig.touchOtherColumn()
	require.NotNil(t, rig.degradedSince())
	assert.True(t, entered.Equal(*rig.degradedSince()), "staying DEGRADED keeps the first moment")

	rig.setStatus(domain.InstanceStatusRunning)
	assert.Nil(t, rig.degradedSince(), "leaving DEGRADED clears the moment")

	rig.setStatus(domain.InstanceStatusDegraded)
	reentered := rig.degradedSince()
	require.NotNil(t, reentered)
	assert.True(t, reentered.After(*entered), "entering DEGRADED again starts a new moment")
}
