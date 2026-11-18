package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestConnectorTaskService_ACallbackForAWorkflowThatNoLongerExistsIsAcknowledged(t *testing.T) {
	for _, outcome := range []string{"complete", "fail"} {
		t.Run(outcome, func(t *testing.T) {
			h := w12bNewConn()
			h.temporal.err = fmt.Errorf("signal workflow %q: %w", h.signalName(outcome), port.ErrWorkflowNotFound)

			require.NoError(t, h.call(outcome), "the worker is told the callback is done")

			assert.Empty(t, h.temporal.signals, "no signal reached a workflow")
			assert.Equal(t, "delivered", h.cache.values[h.key(outcome)], "a redelivery is acknowledged without signalling again")
			assert.Equal(t, h.task.TenantID, h.tasks.reads[0].tenantID)
			assert.Equal(t, h.task.ID, h.tasks.reads[0].id)

			h.temporal.err = nil
			require.NoError(t, h.call(outcome))
			assert.Empty(t, h.temporal.signals, "the redelivery sends nothing")
		})
	}
}

func TestConnectorTaskService_AnotherSignalFailureIsStillReturnedAndTheCallbackRetried(t *testing.T) {
	for _, outcome := range []string{"complete", "fail"} {
		t.Run(outcome, func(t *testing.T) {
			h := w12bNewConn()
			h.temporal.err = errW12bConnSignalDown

			require.ErrorIs(t, h.call(outcome), errW12bConnSignalDown)

			_, held := h.cache.values[h.key(outcome)]
			assert.False(t, held, "the reservation is released so the retry signals")
		})
	}
}

// fxcCtxCache records whether the context of each delete was already cancelled.
type fxcCtxCache struct {
	*w12bConnCache
	delCtxErrs []error
}

func (c *fxcCtxCache) Del(ctx context.Context, keys ...string) error {
	c.delCtxErrs = append(c.delCtxErrs, ctx.Err())
	return c.w12bConnCache.Del(ctx, keys...)
}

// fxcCancellingTemporal cancels the caller's request while the signal is in flight and fails it.
type fxcCancellingTemporal struct {
	port.TemporalClient
	cancel context.CancelFunc
}

func (c *fxcCancellingTemporal) SignalWorkflow(context.Context, string, uuid.UUID, string, any) error {
	c.cancel()
	return context.Canceled
}

func TestConnectorTaskService_AFailedSignalReleasesTheReservationEvenWhenTheRequestWasCancelled(t *testing.T) {
	for _, outcome := range []string{"complete", "fail"} {
		t.Run(outcome, func(t *testing.T) {
			h := w12bNewConn()
			cache := &fxcCtxCache{w12bConnCache: h.cache}
			h.svc.Cache = cache
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h.svc.Temporal = &fxcCancellingTemporal{cancel: cancel}

			var err error
			if outcome == "complete" {
				err = h.svc.Complete(ctx, h.task.TenantID, h.task.ID, map[string]any{})
			} else {
				err = h.svc.Fail(ctx, h.task.TenantID, h.task.ID, "upstream_error", nil)
			}

			require.ErrorIs(t, err, context.Canceled)
			require.NotEmpty(t, cache.delCtxErrs)
			for _, ctxErr := range cache.delCtxErrs {
				assert.NoError(t, ctxErr, "the release is not tied to the cancelled request")
			}
			_, held := h.cache.values[h.key(outcome)]
			assert.False(t, held)
		})
	}
}
