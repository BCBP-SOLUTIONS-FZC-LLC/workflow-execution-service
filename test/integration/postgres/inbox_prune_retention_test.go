//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestInbox_Prune_NonPositiveRetention_ErrorsAndKeepsEveryRow(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership)
	require.NoError(t, err)
	ctx := context.Background()

	recorded := uuid.New()
	require.NoError(t, ledger.MarkProcessed(ctx, recorded, port.ConsumerMembership))

	cases := []struct {
		name      string
		retention time.Duration
	}{
		{"zero retention", 0},
		{"negative retention", -time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deleted, err := ledger.Prune(ctx, tc.retention)

			require.Error(t, err)
			assert.Contains(t, err.Error(), `prune "`+port.ConsumerMembership+`"`)
			assert.True(t, strings.Contains(err.Error(), "Prune requires a positive retention"), err.Error())
			assert.Zero(t, deleted)

			processed, err := ledger.IsProcessed(ctx, recorded, port.ConsumerMembership)
			require.NoError(t, err)
			assert.True(t, processed, "a refused prune must not delete anything")
		})
	}
}
