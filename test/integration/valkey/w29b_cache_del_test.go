//go:build integration

package valkey_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
)

func w29bNewClient(t *testing.T) *redis.Client {
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
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379")
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: host + ":" + port.Port()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestCache_Del_RemovesTheGivenKeysAndToleratesAbsentOnes(t *testing.T) {
	client := w29bNewClient(t)
	cache := valkey.NewCache(client)
	ctx := context.Background()
	for _, k := range []string{"w29b-a", "w29b-b", "w29b-keep"} {
		require.NoError(t, cache.Set(ctx, k, "v-"+k, time.Minute))
	}

	t.Run("several keys at once", func(t *testing.T) {
		require.NoError(t, cache.Del(ctx, "w29b-a", "w29b-b"))

		for _, k := range []string{"w29b-a", "w29b-b"} {
			got, err := cache.Get(ctx, k)
			require.NoError(t, err)
			assert.Empty(t, got, k)
		}
		kept, err := cache.Get(ctx, "w29b-keep")
		require.NoError(t, err)
		assert.Equal(t, "v-w29b-keep", kept, "a key that was not named stays")
	})

	t.Run("an absent key is not an error", func(t *testing.T) {
		require.NoError(t, cache.Del(ctx, "w29b-never-set"))
		require.NoError(t, cache.Del(ctx, "w29b-a", "w29b-never-set"), "deleted twice, plus one that never existed")
	})

	t.Run("the key can be taken again", func(t *testing.T) {
		taken, err := cache.SetNX(ctx, "w29b-a", "again", time.Minute)
		require.NoError(t, err)
		assert.True(t, taken)
		got, err := cache.Get(ctx, "w29b-a")
		require.NoError(t, err)
		assert.Equal(t, "again", got)
	})

	t.Run("one key", func(t *testing.T) {
		require.NoError(t, cache.Del(ctx, "w29b-keep"))
		got, err := cache.Get(ctx, "w29b-keep")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// w29bDelFails reaches the real server for everything but Del, which fails as an unreachable server does.
type w29bDelFails struct {
	redis.Cmdable
	err error
}

func (c w29bDelFails) Del(context.Context, ...string) *redis.IntCmd {
	return redis.NewIntResult(0, c.err)
}

func TestCache_Set_SetsTheTTLAndADeleteThatFailsLeavesTheKeyUntilItExpires(t *testing.T) {
	client := w29bNewClient(t)
	ctx := context.Background()
	cache := valkey.NewCache(client)
	require.NoError(t, cache.Set(ctx, "w29b-held", "reserved", 30*time.Second))

	ttl, err := client.TTL(ctx, "w29b-held").Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "the key expires")
	assert.LessOrEqual(t, ttl, 30*time.Second, "at most the requested ttl")

	cause := errors.New("w29b connection reset")
	failing := valkey.NewCache(w29bDelFails{Cmdable: client, err: cause})

	err = failing.Del(ctx, "w29b-held")

	require.ErrorIs(t, err, cause)
	assert.Contains(t, err.Error(), "valkey del: ")
	got, err := cache.Get(ctx, "w29b-held")
	require.NoError(t, err)
	assert.Equal(t, "reserved", got, "the key stays when the delete fails")
	ttl, err = client.TTL(ctx, "w29b-held").Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "and still expires on its own ttl")
}
