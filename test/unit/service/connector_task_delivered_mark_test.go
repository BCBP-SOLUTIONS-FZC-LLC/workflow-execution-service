package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// cbfixCtxCache refuses writes on a cancelled context, as a real cache client does.
type cbfixCtxCache struct{ *cbfixCache }

func (c *cbfixCtxCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.cbfixCache.Set(ctx, key, value, ttl)
}

// The worker may give up on its request once the signal is sent; the delivered mark is still written, so a later
// retry is acknowledged instead of signalling the workflow a second time.
func TestConnectorTaskService_Complete_RequestCancelledAfterTheSignal_StillMarksDelivered(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	temporal := &fakeTemporalClient{}
	temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error {
		cancel() // the caller's deadline passes right after Temporal accepted the signal
		return nil
	}
	cache := &cbfixCtxCache{&cbfixCache{fakeCacheStore: newFakeCacheStore()}}
	svc := cbfixService(task, temporal, cache, newFakeAssignmentRepo())

	require.NoError(t, svc.Complete(ctx, task.TenantID, task.ID, map[string]any{"x": 1}))

	temporal.signalFunc = nil
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1}))
	assert.Len(t, temporal.signals, 1, "the retry was acknowledged, not signalled again")
}
