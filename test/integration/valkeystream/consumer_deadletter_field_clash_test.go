//go:build integration

package valkeystream_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

func TestConsumer_DeadLetter_FieldsNamedLikeTheAdapterOnesAreReplacedByTheAdapterValues(t *testing.T) {
	client := vsxNewClient(t)
	ctx := context.Background()
	stream := vsxStream(t)
	deadStream := stream + "-dead"
	delivered := vsxPublishAndRead(t, client, stream, vsxClaimerA, 1)
	entry := valkeystream.Entry{ID: delivered[0].ID, Fields: map[string]string{
		"reason": "from-the-producer", "source_entry_id": "0-0", "task_id": "t-1",
	}}

	err := valkeystream.NewConsumer(client).DeadLetter(ctx, stream, vsxGroup, deadStream, entry, "max_deliveries_exceeded")

	require.NoError(t, err)
	dead, err := client.XRange(ctx, deadStream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, dead, 1)
	assert.Equal(t, map[string]any{
		"reason": "max_deliveries_exceeded", "source_entry_id": delivered[0].ID, "task_id": "t-1",
	}, dead[0].Values, "the copy carries the adapter's reason and source id; a producer's value of the same name is lost")
}
