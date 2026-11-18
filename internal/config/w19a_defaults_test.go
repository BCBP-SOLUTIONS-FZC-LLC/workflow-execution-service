package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoad_CleanEnvironment_ProcessIdentityAndPortDefaults(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"nothing set", nil},
		{"APP_ENV and BUILD_VERSION set to the empty string", map[string]string{"APP_ENV": "", "BUILD_VERSION": ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w19aEnv(t, tc.env)

			cfg, err := Load()

			require.NoError(t, err)
			w19aRunChecks(t, []w19aCheck{
				{"environment is dev", cfg.AppEnv, "dev"},
				{"build version is dev", cfg.BuildVersion, "dev"},
				{"http port", cfg.HTTPPort, 8080},
				{"metrics port", cfg.MetricsPort, 9091},
				{"worker health port", cfg.WorkerHealthPort, 8081},
				{"worker metrics port", cfg.WorkerMetricsPort, 8082},
				{"telemetry service name", cfg.OTELServiceName, "workflow-execution-service"},
			})
		})
	}
}

func TestLoad_CleanEnvironment_OutboxRetentionAndSweepDefaults(t *testing.T) {
	w19aEnv(t, nil)

	cfg, err := Load()

	require.NoError(t, err)
	w19aRunChecks(t, []w19aCheck{
		{"outbox poll interval", cfg.OutboxPollInterval, 500 * time.Millisecond},
		{"outbox batch size", cfg.OutboxBatchSize, 50},
		{"rls violation export interval", cfg.RLSViolationExportInterval, 30 * time.Second},
		{"rls violation retention", cfg.RLSViolationRetention, 168 * time.Hour},
		{"outbox retention", cfg.OutboxRetention, 61320 * time.Hour},
		{"tenant history retention", cfg.TenantHistoryRetention, 61320 * time.Hour},
		{"retention sweep interval", cfg.RetentionSweepInterval, 24 * time.Hour},
		{"retention sweep batch", cfg.RetentionSweepBatch, 1000},
		{"operational stats interval", cfg.OperationalStatsInterval, 30 * time.Second},
		{"tenant purge is enabled", cfg.TenantPurgeEnabled, true},
	})
}

func TestLoad_CleanEnvironment_InternalClientDefaults(t *testing.T) {
	w19aEnv(t, nil)

	cfg, err := Load()

	require.NoError(t, err)
	w19aRunChecks(t, []w19aCheck{
		{"definition service url is empty", cfg.DefinitionServiceURL, ""},
		{"definition client timeout", cfg.DefinitionClientTimeout, 5 * time.Second},
		{"eligibility url is empty", cfg.EligibilityBaseURL, ""},
		{"eligibility client timeout", cfg.EligibilityClientTimeout, 5 * time.Second},
		{"token service url is empty", cfg.TokenServiceBaseURL, ""},
		{"token service client timeout", cfg.TokenServiceClientTimeout, 5 * time.Second},
		{"entitlements url is empty", cfg.EntitlementsBaseURL, ""},
		{"entitlements client timeout", cfg.EntitlementsClientTimeout, 5 * time.Second},
		{"entitlements cache ttl", cfg.EntitlementsCacheTTL, 60 * time.Second},
		{"entitlements freshness bound", cfg.EntitlementsFreshnessBound, 10 * time.Minute},
		{"queue topology poll interval", cfg.QueueTopologyPollInterval, 60 * time.Second},
		{"workflow cache size", cfg.WorkflowCacheSize, 0},
		{"glue registry name is empty", cfg.GlueRegistryName, ""},
		{"glue schema cache ttl", cfg.GlueSchemaCacheTTL, 5 * time.Minute},
	})
}
