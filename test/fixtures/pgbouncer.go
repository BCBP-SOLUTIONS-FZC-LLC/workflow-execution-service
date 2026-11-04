package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewPgBouncer(t *testing.T) (bouncerDSN, directDSN string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}
	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("fixtures.NewPgBouncer: network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          "postgres:18-alpine",
			ExposedPorts:   []string{"5432/tcp"},
			Networks:       []string{net.Name},
			NetworkAliases: map[string][]string{net.Name: {"pg"}},
			Env:            map[string]string{"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "testdb"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("fixtures.NewPgBouncer: postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	pgb, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "edoburu/pgbouncer:1.17.0",
			ExposedPorts: []string{"6432/tcp"},
			Networks:     []string{net.Name},
			Env: map[string]string{
				"DB_HOST": "pg", "DB_PORT": "5432", "DB_NAME": "testdb", "DB_USER": "postgres", "DB_PASSWORD": "postgres",
				"POOL_MODE": "transaction", "MAX_CLIENT_CONN": "100", "DEFAULT_POOL_SIZE": "5", "LISTEN_PORT": "6432", "AUTH_TYPE": "plain",
			},
			WaitingFor: wait.ForLog("process up").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("fixtures.NewPgBouncer: pgbouncer: %v", err)
	}
	t.Cleanup(func() { _ = pgb.Terminate(context.Background()) })

	pgHost, _ := pg.Host(ctx)
	pgPort, err := mappedPortWithin(ctx, pg, func(ctx context.Context, c testcontainers.Container) (string, error) {
		p, err := c.MappedPort(ctx, "5432")
		return p.Port(), err
	})
	if err != nil {
		t.Fatalf("fixtures.NewPgBouncer: postgres port: %v", err)
	}
	bHost, _ := pgb.Host(ctx)
	bPort, err := mappedPortWithin(ctx, pgb, func(ctx context.Context, c testcontainers.Container) (string, error) {
		p, err := c.MappedPort(ctx, "6432")
		return p.Port(), err
	})
	if err != nil {
		t.Fatalf("fixtures.NewPgBouncer: pgbouncer port: %v", err)
	}
	return fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable", bHost, bPort),
		fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable", pgHost, pgPort)
}
