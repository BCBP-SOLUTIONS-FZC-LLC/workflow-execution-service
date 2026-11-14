//go:build integration

package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
)

func vsxNewRawClient(t *testing.T) *redis.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("vsxNewRawClient: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379")
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: host + ":" + port.Port()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestCache_KeysAreUsedExactlyAsGiven_NoPrefixOrNamespace(t *testing.T) {
	raw := vsxNewRawClient(t)
	cache := valkey.NewCache(raw)
	ctx := context.Background()

	const (
		setKey   = "vsx-tenant:idempotency:set-key"
		nxKey    = "vsx-nx-key"
		rawKey   = "vsx-written-by-raw-client"
		delKey   = "vsx-del-key"
		otherKey = "vsx-untouched-key"
	)

	require.NoError(t, cache.Set(ctx, setKey, "set-value", time.Minute))
	won, err := cache.SetNX(ctx, nxKey, "nx-value", time.Minute)
	require.NoError(t, err)
	require.True(t, won)

	storedSet, err := raw.Get(ctx, setKey).Result()
	require.NoError(t, err, "Set must store the key exactly as given")
	assert.Equal(t, "set-value", storedSet)
	storedNX, err := raw.Get(ctx, nxKey).Result()
	require.NoError(t, err, "SetNX must store the key exactly as given")
	assert.Equal(t, "nx-value", storedNX)

	keys, err := raw.Keys(ctx, "*").Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{setKey, nxKey}, keys, "no other key, prefixed or not, was written")

	require.NoError(t, raw.Set(ctx, rawKey, "raw-value", time.Minute).Err())
	got, err := cache.Get(ctx, rawKey)
	require.NoError(t, err)
	assert.Equal(t, "raw-value", got, "Get must read the key exactly as given")

	require.NoError(t, raw.Set(ctx, delKey, "to-delete", time.Minute).Err())
	require.NoError(t, raw.Set(ctx, otherKey, "keep", time.Minute).Err())
	require.NoError(t, cache.Del(ctx, delKey))
	exists, err := raw.Exists(ctx, delKey).Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "Del must remove the key exactly as given")
	kept, err := raw.Get(ctx, otherKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "keep", kept)
}
