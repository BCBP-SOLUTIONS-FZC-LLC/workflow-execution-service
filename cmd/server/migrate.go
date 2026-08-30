package main

import (
	"context"
	"fmt"
	"log"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/db/migrations"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/pglogger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// serviceMigrationsTable keeps the service's domain migration history in a
// dedicated tracking table so it never collides with the outbox schema's
// migration history (which ApplySchema tracks under the pgcommon default table).
const serviceMigrationsTable = "wf_execution_migrations"

// runMigrations applies the outbox schema first (its tables are referenced by
// the service's outbox RLS migration), then the service's own domain migrations.
// It must complete before the connection pool is opened and either binary starts.
func runMigrations(ctx context.Context, dsn string, pgLog port.Logger) error {
	log.Println("running migrations")

	adapted := pglogger.New(pgLog)

	// 1. Outbox tables (platform-events owns the DDL; tracked under its own table).
	if err := outbox.ApplySchema(ctx, &migrate.Runner{DSN: dsn, Logger: adapted}); err != nil {
		return fmt.Errorf("outbox schema: %w", err)
	}

	domain := &migrate.Runner{
		FS:              migrations.FS,
		DSN:             dsn,
		MigrationsTable: serviceMigrationsTable,
		Logger:          adapted,
	}
	if err := domain.Up(ctx); err != nil {
		return fmt.Errorf("domain migrations: %w", err)
	}

	log.Println("migrations applied")
	return nil
}
