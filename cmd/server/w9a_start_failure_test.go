package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

const w9aRefusedDSN = "postgres://w9a:w9a-secret@127.0.0.1:1/w9a?sslmode=disable&connect_timeout=2"

func TestServerStart_AnUnreachableAppDatabaseFailsTheStartWithAppDbPoolAndExitsOne(t *testing.T) {
	t.Run("newApp returns the pool error and no app", func(t *testing.T) {
		cfg := &config.Config{
			AppEnv: "test", OTELServiceName: "workflow-execution-service", BuildVersion: "test",
			DatabaseURL: w9aRefusedDSN, PGMaxConns: 2, HTTPPort: 1, MetricsPort: 2,
		}

		got, err := newApp(cfg)

		require.Error(t, err)
		assert.Nil(t, got, "no app, so no listener can be opened")
		assert.True(t, strings.HasPrefix(err.Error(), "app db pool: "), "error = %v", err)
		assert.NotContains(t, err.Error(), "w9a-secret", "the password stays out of the error")
	})

	t.Run("the binary exits one with the cmd/server prefix and opens no listener", func(t *testing.T) {
		res := w9aRunServer(t, []string{
			"APP_ENV=dev",
			"DATABASE_URL=" + w9aRefusedDSN,
			"DEFINITION_SERVICE_URL=http://127.0.0.1:1",
			"ELIGIBILITY_BASE_URL=http://127.0.0.1:1",
			"TOKEN_SERVICE_BASE_URL=http://127.0.0.1:1",
			"SHUTDOWN_DRAIN_DELAY=0s",
		})

		assert.Equal(t, 1, res.exitCode)
		assert.Contains(t, res.stderr, "cmd/server: app db pool: ")
		assert.NotContains(t, res.combined(), "server starting", "no HTTP or metrics listener was started")
		assert.NotContains(t, res.combined(), "w9a-secret")
	})
}
