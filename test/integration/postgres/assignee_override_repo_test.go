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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestAssigneeOverrideRepo_CreateAndListByInstance(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	overrideRepo := postgres.NewAssigneeOverrideRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))

	override := &domain.AssigneeOverride{
		ID:                 uuid.New(),
		TenantID:           tenantA,
		WorkflowInstanceID: inst.ID,
		NodeKey:            "finance/review",
		PreviousUserID:     uuid.New(),
		NewUserID:          uuid.New(),
		Reason:             "primary reviewer on leave",
		ActorUserID:        uuid.New(),
	}
	require.NoError(t, overrideRepo.Create(ctx, override))
	assert.False(t, override.CreatedAt.IsZero())

	overrides, err := overrideRepo.ListByInstance(ctx, tenantA, inst.ID)
	require.NoError(t, err)
	require.Len(t, overrides, 1)
	assert.Equal(t, override.NewUserID, overrides[0].NewUserID)
	assert.Equal(t, "primary reviewer on leave", overrides[0].Reason)

	t.Run("a different instance sees no overrides", func(t *testing.T) {
		otherInst := newInstance(tenantA, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, otherInst))
		overrides, err := overrideRepo.ListByInstance(ctx, tenantA, otherInst.ID)
		require.NoError(t, err)
		assert.Empty(t, overrides)
	})

	t.Run("a nonexistent workflow_instance_id fails the FK constraint", func(t *testing.T) {
		bad := &domain.AssigneeOverride{
			ID:                 uuid.New(),
			TenantID:           tenantA,
			WorkflowInstanceID: uuid.New(),
			NodeKey:            "finance/review",
			PreviousUserID:     uuid.New(),
			NewUserID:          uuid.New(),
			ActorUserID:        uuid.New(),
		}
		err := overrideRepo.Create(ctx, bad)
		assert.Error(t, err)
	})
}
