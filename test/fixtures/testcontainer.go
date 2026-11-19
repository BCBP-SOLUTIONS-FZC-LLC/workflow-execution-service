package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/migrate"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	documentsql "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/documents/sqlstore"
	intentsql "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendintent/sqlstore"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/db/migrations"
)

const serviceMigrationsTable = "wf_execution_migrations"

func NewTestPool(t *testing.T) *pgcommon.Pool {
	pool, _ := NewTestPoolAndDSN(t)
	return pool
}

func NewTestPoolAndDSN(t *testing.T) (*pgcommon.Pool, string) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}

	if shared != nil {
		return shared.newDatabase(t)
	}

	ctx := context.Background()
	container, host, port, err := startWithMappedPort(ctx, postgresRequest(), func(ctx context.Context, c testcontainersContainer) (string, error) {
		p, err := c.MappedPort(ctx, "5432")
		return p.Port(), err
	})
	if err != nil {
		t.Fatalf("fixtures.NewTestPoolAndDSN: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable", host, port)

	applyMigrations(t, dsn)

	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn})
	if err != nil {
		t.Fatalf("fixtures.NewTestPoolAndDSN: create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool, dsn
}

type testcontainersContainer = testcontainers.Container

func postgresRequest() testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image:        "postgres:18-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(120 * time.Second),
	}
}

func applyMigrations(t *testing.T, dsn string) {
	t.Helper()
	if err := applyMigrationsTo(context.Background(), dsn); err != nil {
		t.Fatalf("fixtures: %v", err)
	}
}

func applyMigrationsTo(ctx context.Context, dsn string) error {
	if err := outbox.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		return fmt.Errorf("outbox ApplySchema: %w", err)
	}
	if err := inbox.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		return fmt.Errorf("inbox ApplySchema: %w", err)
	}
	if err := documentsql.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		return fmt.Errorf("connector documents ApplySchema: %w", err)
	}
	if err := intentsql.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		return fmt.Errorf("connector send intents ApplySchema: %w", err)
	}
	domainRunner := &migrate.Runner{
		FS:              migrations.FS,
		DSN:             dsn,
		MigrationsTable: serviceMigrationsTable,
	}
	if err := domainRunner.Up(ctx); err != nil {
		return fmt.Errorf("domain migrations: %w", err)
	}
	return nil
}

const (
	startAttempts   = 3
	portMapTimeout  = 20 * time.Second
	portMapInterval = 200 * time.Millisecond
)

func startWithMappedPort(
	ctx context.Context,
	req testcontainers.ContainerRequest,
	lookup func(context.Context, testcontainers.Container) (string, error),
) (testcontainers.Container, string, string, error) {
	var lastErr error
	for attempt := 0; attempt < startAttempts; attempt++ {
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			lastErr = err
			if container != nil {
				_ = container.Terminate(ctx)
			}
			continue
		}
		host, err := container.Host(ctx)
		if err == nil {
			var port string
			if port, err = mappedPortWithin(ctx, container, lookup); err == nil {
				return container, host, port, nil
			}
		}
		lastErr = err
		_ = container.Terminate(ctx)
	}
	return nil, "", "", fmt.Errorf("a container with a published port after %d attempts: %w", startAttempts, lastErr)
}

func mappedPortWithin(ctx context.Context, container testcontainers.Container, lookup func(context.Context, testcontainers.Container) (string, error)) (string, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, portMapTimeout)
	defer cancel()
	var lastErr error
	for lookupCtx.Err() == nil {
		port, err := lookup(lookupCtx, container)
		if err == nil {
			return port, nil
		}
		lastErr = err
		select {
		case <-lookupCtx.Done():
		case <-time.After(portMapInterval):
		}
	}
	return "", lastErr
}
