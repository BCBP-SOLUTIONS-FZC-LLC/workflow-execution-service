package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewTestValkey starts a throwaway Valkey instance (the same image
// docker-compose.yml's own valkey service uses) and returns its "host:port"
// address. Skipped under -short, same convention as NewTestPool/
// NewTestTemporalServer.
func NewTestValkey(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("fixtures.NewTestValkey: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("fixtures.NewTestValkey: container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("fixtures.NewTestValkey: mapped port: %v", err)
	}
	addr := host + ":" + port.Port()

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixtures.NewTestValkey: server never became reachable: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	return addr
}
