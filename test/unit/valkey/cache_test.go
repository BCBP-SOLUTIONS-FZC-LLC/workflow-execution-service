// Package valkey_test covers internal/adapter/outbound/valkey.Cache's error
// paths without a real Valkey instance — connecting to a port nothing
// listens on forces every command to fail immediately. The happy paths are
// covered against a real container in test/integration/valkey.
package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
)

func newUnreachableCache() *valkey.Cache {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
	})
	return valkey.NewCache(client)
}

func TestCache_Get_ConnectionError(t *testing.T) {
	_, err := newUnreachableCache().Get(context.Background(), "k")
	assert.Error(t, err)
}

func TestCache_Set_ConnectionError(t *testing.T) {
	err := newUnreachableCache().Set(context.Background(), "k", "v", time.Minute)
	assert.Error(t, err)
}

func TestCache_Del_ConnectionError(t *testing.T) {
	err := newUnreachableCache().Del(context.Background(), "k")
	assert.Error(t, err)
}

func TestCache_SetNX_ConnectionError(t *testing.T) {
	_, err := newUnreachableCache().SetNX(context.Background(), "k", "v", time.Minute)
	assert.Error(t, err)
}

func TestCache_Ping_ConnectionError(t *testing.T) {
	err := newUnreachableCache().Ping(context.Background())
	assert.Error(t, err)
}
