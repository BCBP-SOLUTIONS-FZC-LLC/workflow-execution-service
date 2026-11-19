package valkey_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
)

// w29bUnreachableCache talks to a port nothing listens on and never retries, so each call fails once.
func w29bUnreachableCache(t *testing.T) *valkey.Cache {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: time.Second, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return valkey.NewCache(client)
}

// w29bCacheCalls runs each cache operation once and hands back whether it returned no value (always true
// for an operation that returns only an error) and its error. returnsValue marks Get and SetNX, whose
// empty value or false answer is asserted.
var w29bCacheCalls = []struct {
	name         string
	wantPrefix   string
	returnsValue bool
	call         func(ctx context.Context, c *valkey.Cache) (noValue bool, err error)
}{
	{"get", "valkey get: ", true, func(ctx context.Context, c *valkey.Cache) (bool, error) {
		val, err := c.Get(ctx, "w29b-key")
		return val == "", err
	}},
	{"set", "valkey set: ", false, func(ctx context.Context, c *valkey.Cache) (bool, error) {
		return true, c.Set(ctx, "w29b-key", "v", time.Minute)
	}},
	{"setnx", "valkey setnx: ", true, func(ctx context.Context, c *valkey.Cache) (bool, error) {
		taken, err := c.SetNX(ctx, "w29b-key", "v", time.Minute)
		return !taken, err
	}},
	{"del", "valkey del: ", false, func(ctx context.Context, c *valkey.Cache) (bool, error) {
		return true, c.Del(ctx, "w29b-key", "w29b-other")
	}},
	{"ping", "valkey ping: ", false, func(ctx context.Context, c *valkey.Cache) (bool, error) {
		return true, c.Ping(ctx)
	}},
}

func TestCache_UnreachableServer_WrapsTheCauseWithTheOperation(t *testing.T) {
	for _, tc := range w29bCacheCalls {
		t.Run(tc.name, func(t *testing.T) {
			noValue, err := tc.call(context.Background(), w29bUnreachableCache(t))

			require.Error(t, err)
			if tc.returnsValue {
				assert.True(t, noValue, "no value and no reservation come back with the error")
			}
			assert.True(t, strings.HasPrefix(err.Error(), tc.wantPrefix), "the operation prefix comes first: %q", err.Error())
			opErr, ok := errors.AsType[*net.OpError](err)
			require.True(t, ok, "the dial failure stays reachable behind the prefix")
			assert.Equal(t, "dial", opErr.Op)
		})
	}
}

func TestCache_CallerContextEnded_KeepsTheContextErrorBehindThePrefix(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expire := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer expire()
	contexts := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"cancelled", cancelled, context.Canceled},
		{"deadline passed", expired, context.DeadlineExceeded},
	}
	for _, c := range contexts {
		for _, op := range w29bCacheCalls {
			t.Run(op.name+" "+c.name, func(t *testing.T) {
				noValue, err := op.call(c.ctx, w29bUnreachableCache(t))

				require.Error(t, err)
				if op.returnsValue {
					assert.True(t, noValue)
				}
				assert.ErrorIs(t, err, c.want)
				assert.Contains(t, err.Error(), op.wantPrefix)
			})
		}
	}
}
