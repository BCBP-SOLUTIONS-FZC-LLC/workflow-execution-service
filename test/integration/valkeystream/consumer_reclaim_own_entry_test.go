//go:build integration

package valkeystream_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

func TestConsumer_ReclaimStale_TheConsumerThatOwnsAnIdleEntryGetsItAgainAndItsDeliveryCountRises(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	entries := vsxPublishAndRead(t, client, stream, vsxClaimerA, 1)
	consumer := valkeystream.NewConsumer(client)
	vsxMakeIdle(t, client, stream, vsxClaimerA, []string{entries[0].ID}, vsxLongIdle)
	before := vsxPending(t, client, stream)
	require.Len(t, before, 1)

	reclaimed, _, err := consumer.ReclaimStale(ctx, stream, vsxGroup, vsxClaimerA, "0", vsxMinIdle, vsxReadBatch)

	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	assert.Equal(t, entries[0].ID, reclaimed[0].ID)
	after := vsxPending(t, client, stream)
	require.Len(t, after, 1)
	assert.Equal(t, vsxClaimerA, after[0].Consumer)
	assert.Equal(t, before[0].RetryCount+1, after[0].RetryCount, "the reclaim counts as a delivery")
}
