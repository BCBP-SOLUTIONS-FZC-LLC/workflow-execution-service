//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTransactor_RunInTxWithRetry_TwoConflictingSerializableTransactions_BothEndUpCommitted(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	instances := postgres.NewInstanceRepo(appPool)

	tenantID := uuid.New()
	ctx := withGUC(context.Background(), tenantID)
	require.NoError(t, instances.Create(context.Background(), newInstance(tenantID, time.Now().UTC())))

	var readers atomic.Int32
	bothHaveRead := make(chan struct{})
	var attempts atomic.Int32
	run := func(inst *domain.Instance) error {
		firstAttempt := true
		return transactor.RunInTxWithRetry(ctx, func(txCtx context.Context) error {
			attempts.Add(1)
			if _, _, err := instances.ListByTenant(txCtx, tenantID, port.InstanceListFilter{}, port.PageRequest{}); err != nil {
				return err
			}
			if firstAttempt {
				firstAttempt = false
				if readers.Add(1) == 2 {
					close(bothHaveRead)
				}
				select {
				case <-bothHaveRead:
				case <-time.After(10 * time.Second):
					return errors.New("the other transaction never reached the barrier")
				}
			}
			return instances.Create(txCtx, inst)
		})
	}

	first, second := newInstance(tenantID, time.Now().UTC()), newInstance(tenantID, time.Now().UTC())
	results := make(chan error, 2)
	for _, inst := range []*domain.Instance{first, second} {
		go func() { results <- run(inst) }()
	}
	require.NoError(t, <-results)
	require.NoError(t, <-results)

	assert.GreaterOrEqual(t, attempts.Load(), int32(3), "one of the two conflicting transactions ran again")
	for _, inst := range []*domain.Instance{first, second} {
		got, err := instances.GetByID(context.Background(), tenantID, inst.ID)
		require.NoError(t, err)
		assert.Equal(t, inst.BusinessKey, got.BusinessKey)
	}
}

func TestTransactor_RunInTxWithRetry_AMappedUniqueViolation_IsReturnedAfterOneAttempt(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	transactor := postgres.NewTransactor(appPool)
	instances := postgres.NewInstanceRepo(appPool)

	tenantID := uuid.New()
	ctx := withGUC(context.Background(), tenantID)
	existing := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instances.Create(context.Background(), existing))

	duplicate := newInstance(tenantID, time.Now().UTC())
	duplicate.SubjectID, duplicate.BusinessKey = existing.SubjectID, existing.BusinessKey
	var attempts atomic.Int32

	err := transactor.RunInTxWithRetry(ctx, func(txCtx context.Context) error {
		attempts.Add(1)
		return instances.Create(txCtx, duplicate)
	})

	require.ErrorIs(t, err, domain.ErrDuplicateBusinessKey)
	assert.EqualValues(t, 1, attempts.Load(), "a client-level conflict is not retried")
	_, getErr := instances.GetByID(context.Background(), tenantID, duplicate.ID)
	assert.ErrorIs(t, getErr, domain.ErrNotFound)
}
