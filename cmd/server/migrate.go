package main

import (
	"context"
	"fmt"
	"log"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/migrate"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/db/migrations"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/pglogger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const serviceMigrationsTable = "wf_execution_migrations"

func runMigrations(ctx context.Context, dsn string, pgLog port.Logger) error {
	log.Println("running migrations")

	adapted := pglogger.New(pgLog)

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
