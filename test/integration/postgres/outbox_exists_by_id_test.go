//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestOutboxRepo_ExistsByID_FindsOnlyAnEventOfTheCallersTenant(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	tenantA, tenantB := uuid.New(), uuid.New()
	env := instanceEventEnvelope(tenantA, uuid.New())
	eventID := uuid.MustParse(env.ID)
	require.NoError(t, transactor.RunInTx(withGUC(context.Background(), tenantA), func(ctx context.Context) error {
		return outboxRepo.Enqueue(ctx, env)
	}))
	ctx := context.Background()

	found, err := outboxRepo.ExistsByID(ctx, tenantA, eventID)
	require.NoError(t, err)
	assert.True(t, found, "the event is found by its id")

	found, err = outboxRepo.ExistsByID(ctx, tenantA, uuid.New())
	require.NoError(t, err)
	assert.False(t, found, "an id nobody enqueued is not found")

	found, err = outboxRepo.ExistsByID(ctx, tenantB, eventID)
	require.NoError(t, err)
	assert.False(t, found, "another tenant's event id is not found")
}
