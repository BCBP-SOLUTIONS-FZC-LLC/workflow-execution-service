package main

import (
	"context"
	"fmt"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

var version = "dev"

func main() {
	if config.VersionRequested() {
		fmt.Println(version)
		return
	}

	cmd, err := parseCommand(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	if cmd == migrateCommand {
		if err := cfg.RequireMigrate(); err != nil {
			fmt.Fprintf(os.Stderr, "config error: %v\n", err)
			os.Exit(1)
		}
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

	if err := cfg.RequireServer(); err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	a, err := newApp(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cmd/server: %v\n", err)
		os.Exit(1)
	}
	if err := a.run(); err != nil {
		fmt.Fprintf(os.Stderr, "cmd/server: %v\n", err)
		os.Exit(1)
	}
}
