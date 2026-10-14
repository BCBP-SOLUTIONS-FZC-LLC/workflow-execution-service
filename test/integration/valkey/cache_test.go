//go:build integration

// Package valkey_test exercises internal/adapter/outbound/valkey.Cache
// against a real Valkey container (LLD §5.9's Idempotency-Key cache),
// matching TESTCONTAINERS_VALKEY_IMAGE already declared in the Makefile.
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

func newTestCache(t *testing.T) *valkey.Cache {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("valkey_test.newTestCache: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("valkey_test.newTestCache: container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("valkey_test.newTestCache: mapped port: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: host + ":" + port.Port()})
	t.Cleanup(func() { _ = client.Close() })

	return valkey.NewCache(client)
}

func TestCache_SetGet(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "k1", "v1", time.Minute))

	got, err := cache.Get(ctx, "k1")
	require.NoError(t, err)
	assert.Equal(t, "v1", got)
}

func TestCache_Get_Miss(t *testing.T) {
	cache := newTestCache(t)

	got, err := cache.Get(context.Background(), "does-not-exist")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCache_Del(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "k2", "v2", time.Minute))
	require.NoError(t, cache.Del(ctx, "k2"))

	got, err := cache.Get(ctx, "k2")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCache_SetNX(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()

	first, err := cache.SetNX(ctx, "k3", "v3", time.Minute)
	require.NoError(t, err)
	assert.True(t, first)

	second, err := cache.SetNX(ctx, "k3", "other", time.Minute)
	require.NoError(t, err)
	assert.False(t, second)

	got, err := cache.Get(ctx, "k3")
	require.NoError(t, err)
	assert.Equal(t, "v3", got, "SetNX must not overwrite an existing key")
}

func TestCache_Ping(t *testing.T) {
	cache := newTestCache(t)

	assert.NoError(t, cache.Ping(context.Background()))
}
