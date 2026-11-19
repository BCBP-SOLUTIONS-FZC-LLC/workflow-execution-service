//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// requireW26bConsumerError fails unless err starts with the consumer's name wrapped as the inbox wraps it and
// carries a cause after that prefix.
func requireW26bConsumerError(t *testing.T, err error, consumer string) {
	t.Helper()
	require.Error(t, err)
	prefix := `consumer "` + consumer + `": `
	assert.True(t, strings.HasPrefix(err.Error(), prefix), "error %q must start with %q", err.Error(), prefix)
	assert.Greater(t, len(err.Error()), len(prefix), "the cause follows the consumer's name")
}

func TestInbox_TheStoreFails_TheErrorNamesTheConsumerAndTheReadAnswersFalse(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	ledger, err := eventbus.NewInbox(pool, port.ConsumerMembership, port.ConsumerUser)
	require.NoError(t, err)
	consumers := []string{port.ConsumerMembership, port.ConsumerUser}

	t.Run("the caller's context is cancelled", func(t *testing.T) {
		for _, consumer := range consumers {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			eventID := uuid.New()

			processed, err := ledger.IsProcessed(ctx, eventID, consumer)

			requireW26bConsumerError(t, err, consumer)
			assert.ErrorIs(t, err, context.Canceled, "the cause stays reachable through the wrapping")
			assert.False(t, processed, "a failed read is not an answer that the event was seen")

			err = ledger.MarkProcessed(ctx, eventID, consumer)

			requireW26bConsumerError(t, err, consumer)
			assert.ErrorIs(t, err, context.Canceled)

			seen, err := ledger.IsProcessed(context.Background(), eventID, consumer)
			require.NoError(t, err)
			assert.False(t, seen, "a failed write recorded nothing")
		}
	})

	t.Run("the database is unreachable", func(t *testing.T) {
		pool.Close()
		for _, consumer := range consumers {
			eventID := uuid.New()

			processed, err := ledger.IsProcessed(context.Background(), eventID, consumer)

			requireW26bConsumerError(t, err, consumer)
			assert.False(t, processed, "an outage must not read as a seen event")
			assert.False(t, errors.Is(err, context.Canceled), "an outage is not a cancellation")

			requireW26bConsumerError(t, ledger.MarkProcessed(context.Background(), eventID, consumer), consumer)
		}
	})
}
