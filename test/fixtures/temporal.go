package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"
)

// NewTestTemporalServer starts a throwaway Temporal dev server (single
// container, in-memory persistence — the same image/command
// docker-compose.yml's own temporal service uses) and returns a connected
// client.Client. Skipped under -short, same convention as NewTestPool.
func NewTestTemporalServer(t *testing.T) client.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "temporalio/temporal:latest",
		Cmd:          []string{"server", "start-dev", "--ip", "0.0.0.0"},
		ExposedPorts: []string{"7233/tcp"},
		WaitingFor:   wait.ForListeningPort("7233/tcp").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("fixtures.NewTestTemporalServer: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("fixtures.NewTestTemporalServer: container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "7233")
	if err != nil {
		t.Fatalf("fixtures.NewTestTemporalServer: mapped port: %v", err)
	}
	hostPort := host + ":" + port.Port()

	// The gRPC port opens before start-dev's namespace registration finishes
	// — retry Dial+CheckHealth rather than treating the port as ready.
	var sdk client.Client
	deadline := time.Now().Add(60 * time.Second)
	for {
		sdk, err = client.Dial(client.Options{HostPort: hostPort})
		if err == nil {
			if _, healthErr := sdk.CheckHealth(ctx, &client.CheckHealthRequest{}); healthErr == nil {
				break
			}
			sdk.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixtures.NewTestTemporalServer: server never became healthy: %v", err)
		}
		time.Sleep(time.Second)
	}
	t.Cleanup(sdk.Close)

	return sdk
}
