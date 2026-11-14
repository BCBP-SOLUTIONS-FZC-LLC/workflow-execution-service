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
)

func TestInstanceRepo_FailUnstarted(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()

	seed := func(t *testing.T) *domain.Instance {
		t.Helper()
		inst := newInstance(tenantA, time.Now().UTC())
		require.NoError(t, repo.Create(ctx, inst))
		return inst
	}
	read := func(t *testing.T, inst *domain.Instance) *domain.Instance {
		t.Helper()
		got, err := repo.GetByID(ctx, tenantA, inst.ID)
		require.NoError(t, err)
		return got
	}

	t.Run("a RUNNING row with no run id is closed once and frees its business key", func(t *testing.T) {
		inst := seed(t)

		changed, err := repo.FailUnstarted(ctx, tenantA, inst.ID)

		require.NoError(t, err)
		assert.True(t, changed)
		got := read(t, inst)
		assert.Equal(t, domain.InstanceStatusFailed, got.Status)
		assert.NotNil(t, got.CompletedAt)
		assert.Equal(t, inst.RecordVersion+1, got.RecordVersion)
		_, err = repo.GetLiveByBusinessKey(ctx, tenantA, inst.SubjectID, inst.BusinessKey)
		assert.ErrorIs(t, err, domain.ErrNotFound)

		again, err := repo.FailUnstarted(ctx, tenantA, inst.ID)
		require.NoError(t, err)
		assert.False(t, again, "a second close changes nothing")
		assert.Equal(t, got.RecordVersion, read(t, inst).RecordVersion)
	})

	t.Run("a row whose workflow has a run id is left alone", func(t *testing.T) {
		inst := seed(t)
		require.NoError(t, repo.RecordRunID(ctx, tenantA, inst.ID, "run-1"))

		changed, err := repo.FailUnstarted(ctx, tenantA, inst.ID)

		require.NoError(t, err)
		assert.False(t, changed)
		assert.Equal(t, domain.InstanceStatusRunning, read(t, inst).Status)
	})

	t.Run("a row stored with an empty run id counts as unstarted", func(t *testing.T) {
		inst := seed(t)
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, `UPDATE workflow_instance SET temporal_run_id = '' WHERE id = $1`, inst.ID)
			return err
		}))

		changed, err := repo.FailUnstarted(ctx, tenantA, inst.ID)

		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("a terminated row is left alone", func(t *testing.T) {
		inst := seed(t)
		_, err := repo.UpdateStatus(ctx, tenantA, inst.ID, domain.InstanceStatusTerminated, inst.RecordVersion)
		require.NoError(t, err)

		changed, err := repo.FailUnstarted(ctx, tenantA, inst.ID)

		require.NoError(t, err)
		assert.False(t, changed)
		assert.Equal(t, domain.InstanceStatusTerminated, read(t, inst).Status)
	})

	t.Run("a paused row is left alone", func(t *testing.T) {
		inst := seed(t)
		_, err := repo.UpdateStatus(ctx, tenantA, inst.ID, domain.InstanceStatusPaused, inst.RecordVersion)
		require.NoError(t, err)

		changed, err := repo.FailUnstarted(ctx, tenantA, inst.ID)

		require.NoError(t, err)
		assert.False(t, changed)
		assert.Equal(t, domain.InstanceStatusPaused, read(t, inst).Status)
	})

	t.Run("another tenant cannot close it and an unknown id changes nothing", func(t *testing.T) {
		inst := seed(t)

		changed, err := repo.FailUnstarted(ctx, tenantB, inst.ID)
		require.NoError(t, err)
		assert.False(t, changed)
		assert.Equal(t, domain.InstanceStatusRunning, read(t, inst).Status)

		changed, err = repo.FailUnstarted(ctx, tenantA, uuid.New())
		require.NoError(t, err)
		assert.False(t, changed)
	})
}
