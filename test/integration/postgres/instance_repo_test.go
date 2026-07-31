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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func newInstance(tenantID uuid.UUID, createdAt time.Time) *domain.Instance {
	id := uuid.New()
	return &domain.Instance{
		ID:                 id,
		TenantID:           tenantID,
		WorkflowID:         uuid.New(),
		WorkflowVersionID:  uuid.New(),
		BusinessKey:        "bk-" + id.String(),
		TemporalWorkflowID: "wf-" + id.String(),
		Status:             domain.InstanceStatusRunning,
		CurrentNodeKeys:    []string{"review"},
		TaskQueue:          "wf-queue-default",
		StartedByUserID:    uuid.New(),
		StartedAt:          &createdAt,
	}
}

// seedInstanceAt inserts inst then back-dates created_at directly (as
// superuser) so list-pagination tests get deterministic, well-spaced
// ordering instead of relying on now()'s clock resolution.
func seedInstanceAt(
	t *testing.T,
	superPool interface {
		WithConn(context.Context, func(context.Context, *pgxpool.Conn) error) error
	},
	ctx context.Context,
	id uuid.UUID,
	createdAt time.Time,
) {
	t.Helper()
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE workflow_instance SET created_at = $2 WHERE id = $1`, id, createdAt)
		return err
	}))
}

func TestInstanceRepo_CreateAndGetByID(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	tenantB := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())

	require.NoError(t, repo.Create(ctx, inst))
	assert.Equal(t, int64(1), inst.RecordVersion)

	t.Run("same tenant can read it back", func(t *testing.T) {
		got, err := repo.GetByID(ctx, tenantA, inst.ID)
		require.NoError(t, err)
		assert.Equal(t, inst.BusinessKey, got.BusinessKey)
	})

	t.Run("cross tenant sees not found", func(t *testing.T) {
		_, err := repo.GetByID(ctx, tenantB, inst.ID)
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("duplicate business key for an active instance is rejected", func(t *testing.T) {
		dup := newInstance(tenantA, time.Now().UTC())
		dup.BusinessKey = inst.BusinessKey
		err := repo.Create(ctx, dup)
		assert.ErrorIs(t, err, domain.ErrDuplicateBusinessKey)
	})
}

func TestInstanceRepo_UpdateStatus(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, repo.Create(ctx, inst))

	t.Run("correct record_version succeeds and bumps it", func(t *testing.T) {
		updated, err := repo.UpdateStatus(ctx, tenantA, inst.ID, domain.InstanceStatusPaused, inst.RecordVersion)
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusPaused, updated.Status)
		assert.Equal(t, inst.RecordVersion+1, updated.RecordVersion)
	})

	t.Run("stale record_version returns a conflict, not not-found", func(t *testing.T) {
		_, err := repo.UpdateStatus(ctx, tenantA, inst.ID, domain.InstanceStatusCompleted, inst.RecordVersion)
		assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	})

	t.Run("nonexistent id returns not-found", func(t *testing.T) {
		_, err := repo.UpdateStatus(ctx, tenantA, uuid.New(), domain.InstanceStatusPaused, 1)
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})
}

func TestInstanceRepo_UpdateStatus_InvalidEnumSurfacesError(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, repo.Create(ctx, inst))

	_, err := repo.UpdateStatus(ctx, tenantA, inst.ID, domain.InstanceStatus("NOT_A_REAL_STATUS"), inst.RecordVersion)
	assert.Error(t, err)
}

func TestInstanceRepo_ListByTenant_KeysetPagination(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	base := time.Now().UTC().Add(-time.Hour)

	var seeded []uuid.UUID
	for i := 0; i < 5; i++ {
		inst := newInstance(tenantA, base)
		require.NoError(t, repo.Create(ctx, inst))
		seedInstanceAt(t, superPool, ctx, inst.ID, base.Add(time.Duration(i)*time.Minute))
		seeded = append(seeded, inst.ID)
	}
	var collected []uuid.UUID
	var cursor *port.Cursor
	for {
		page, next, err := repo.ListByTenant(ctx, tenantA, port.PageRequest{After: cursor, Limit: 2})
		require.NoError(t, err)
		for _, inst := range page {
			collected = append(collected, inst.ID)
		}
		if next == nil {
			break
		}
		cursor = next
	}

	require.Len(t, collected, 5)
	for i, id := range collected {
		assert.Equal(t, seeded[4-i], id, "page order mismatch at position %d", i)
	}

	t.Run("concurrent insert of a newer row doesn't shift an already-fetched page", func(t *testing.T) {
		firstPage, next, err := repo.ListByTenant(ctx, tenantA, port.PageRequest{Limit: 2})
		require.NoError(t, err)
		require.Len(t, firstPage, 2)
		require.NotNil(t, next)

		fresh := newInstance(tenantA, time.Now().UTC())
		require.NoError(t, repo.Create(ctx, fresh))
		seedInstanceAt(t, superPool, ctx, fresh.ID, base.Add(10*time.Minute))

		secondPage, _, err := repo.ListByTenant(ctx, tenantA, port.PageRequest{After: next, Limit: 2})
		require.NoError(t, err)
		for _, inst := range secondPage {
			assert.NotEqual(t, fresh.ID, inst.ID, "the newer concurrently-inserted row must not leak into an older page")
			for _, already := range firstPage {
				assert.NotEqual(t, already.ID, inst.ID, "second page must not repeat a row from the first page")
			}
		}
	})
}
