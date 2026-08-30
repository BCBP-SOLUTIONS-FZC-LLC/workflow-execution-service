package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	// The `migrate` subcommand applies the schema and exits, so it can run as a
	// dedicated migration entrypoint (Kubernetes init container or pre-boot job)
	// rather than in the service's boot path.
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		migrateLog, err := logger.NewLogger(cfg.AppEnv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
			os.Exit(1)
		}
		if err := runMigrations(context.Background(), cfg.MigrationDSN(), migrateLog); err != nil {
			fmt.Fprintf(os.Stderr, "migrate error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	a, err := newApp(cfg)
	if err != nil {
		log.Fatalf("cmd/server: %v", err)
	}
	a.run()
}
