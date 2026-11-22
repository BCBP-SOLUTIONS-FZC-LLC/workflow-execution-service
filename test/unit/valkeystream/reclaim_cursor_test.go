package valkeystream_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

type autoClaimAnswer struct{ next string }

func (a autoClaimAnswer) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (a autoClaimAnswer) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if claim, ok := cmd.(*redis.XAutoClaimCmd); ok {
			claim.SetVal(nil, a.next)
			return nil
		}
		return next(ctx, cmd)
	}
}

func (a autoClaimAnswer) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestConsumer_ReclaimStale_AnEmptyNextCursorStartsOver(t *testing.T) {
	tests := []struct {
		name     string
		answered string
		want     string
	}{
		{name: "an empty cursor", answered: "", want: "0-0"},
		{name: "the end of the list", answered: "0-0", want: "0-0"},
		{name: "a position in the list", answered: "1700-1", want: "1700-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
			client.AddHook(autoClaimAnswer{next: tt.answered})
			consumer := valkeystream.NewConsumer(client)

			entries, next, err := consumer.ReclaimStale(context.Background(), "stream", "group", "consumer", "1600-0", time.Second, 10)

			require.NoError(t, err)
			assert.Empty(t, entries)
			assert.Equal(t, tt.want, next)
		})
	}
}
