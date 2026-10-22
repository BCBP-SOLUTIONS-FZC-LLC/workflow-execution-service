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

func TestTransactor_RunInTxWithRetry(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	inst := newInstance(tenantA, time.Now().UTC())

	require.NoError(t, transactor.RunInTxWithRetry(withGUC(ctx, tenantA), func(ctx context.Context) error {
		return instanceRepo.Create(ctx, inst)
	}))

	got, err := instanceRepo.GetByID(ctx, tenantA, inst.ID)
	require.NoError(t, err)
	assert.Equal(t, inst.BusinessKey, got.BusinessKey)
}
