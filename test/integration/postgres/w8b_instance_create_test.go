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

func TestInstanceRepo_Create_WithEmptyOptionalJSONAndNodeKeys_StoresNullOrRefusesByColumn(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	cases := []struct {
		name       string
		shape      func(inst *domain.Instance)
		wantStored bool
	}{
		{
			name:       "no context and no override map are stored as null and read back as nil",
			shape:      func(inst *domain.Instance) { inst.ContextJSON, inst.OverrideMap = nil, nil },
			wantStored: true,
		},
		{
			name:       "no current node keys is refused by the column and nothing is stored",
			shape:      func(inst *domain.Instance) { inst.CurrentNodeKeys = nil },
			wantStored: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newInstance(tenantID, time.Now().UTC())
			tc.shape(inst)

			err := instances.Create(ctx, inst)

			if !tc.wantStored {
				require.Error(t, err)
				for _, answer := range []error{domain.ErrAlreadyExists, domain.ErrDuplicateBusinessKey, domain.ErrNotFound} {
					assert.NotErrorIs(t, err, answer, "a refused insert must not look like a client answer")
				}
				_, getErr := instances.GetByID(ctx, tenantID, inst.ID)
				assert.ErrorIs(t, getErr, domain.ErrNotFound)
				return
			}
			require.NoError(t, err)
			assert.Nil(t, inst.ContextJSON, "the row returned by the insert holds null")
			assert.Nil(t, inst.OverrideMap)
			got, getErr := instances.GetByID(ctx, tenantID, inst.ID)
			require.NoError(t, getErr)
			assert.Nil(t, got.ContextJSON)
			assert.Nil(t, got.OverrideMap)
			assert.Equal(t, []string{"review"}, got.CurrentNodeKeys)
		})
	}
}
