package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

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

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
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

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("fixtures.NewTestPoolAndDSN: start container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("fixtures.NewTestPoolAndDSN: container host: %v", err)
	}
	port, err := mappedPort(func() (string, error) {
		p, err := container.MappedPort(ctx, "5432")
		return p.Port(), err
	})
	if err != nil {
		t.Fatalf("fixtures.NewTestPoolAndDSN: mapped port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable", host, port)

	applyMigrations(t, dsn)

	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn})
	if err != nil {
		t.Fatalf("fixtures.NewTestPoolAndDSN: create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool, dsn
}

func applyMigrations(t *testing.T, dsn string) {
	t.Helper()

	ctx := context.Background()
	if err := outbox.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		t.Fatalf("fixtures: outbox ApplySchema: %v", err)
	}
	if err := documentsql.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		t.Fatalf("fixtures: connector documents ApplySchema: %v", err)
	}
	if err := intentsql.ApplySchema(ctx, &migrate.Runner{DSN: dsn}); err != nil {
		t.Fatalf("fixtures: connector send intents ApplySchema: %v", err)
	}
	domainRunner := &migrate.Runner{
		FS:              migrations.FS,
		DSN:             dsn,
		MigrationsTable: serviceMigrationsTable,
	}
	if err := domainRunner.Up(ctx); err != nil {
		t.Fatalf("fixtures: domain migrations: %v", err)
	}
}

func mappedPort(lookup func() (string, error)) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 60; attempt++ {
		p, err := lookup()
		if err == nil {
			return p, nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return "", lastErr
}
