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

func TestInstanceRepo_Create_AnExistingInstanceIdAnswersAlreadyExists(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	first := newInstance(uuid.New(), time.Now().UTC())
	require.NoError(t, instances.Create(ctx, first))

	again := newInstance(first.TenantID, time.Now().UTC())
	again.ID = first.ID

	err := instances.Create(ctx, again)

	require.ErrorIs(t, err, domain.ErrAlreadyExists)
	assert.NotErrorIs(t, err, domain.ErrDuplicateBusinessKey, "a different business key must not look like a business-key clash")
}
