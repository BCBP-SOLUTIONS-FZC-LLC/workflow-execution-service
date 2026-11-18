package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// w10aCleanEnv blanks every variable of the process, then the settings the defaults below depend on, so the
// machine's own environment cannot change the answer or fail validation. An empty variable counts as unset
// for every setting.
func w10aCleanEnv(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		if key, _, found := strings.Cut(entry, "="); found && key != "" {
			t.Setenv(key, "")
		}
	}
	for _, key := range []string{
		"APP_ENV", "HTTP_PORT", "METRICS_PORT", "WORKER_HEALTH_PORT", "WORKER_METRICS_PORT",
		"DATABASE_URL", "PG_MAX_CONNS", "PG_MIN_CONNS", "PG_SLOW_QUERY_THRESHOLD_MS", "PG_BOUNCER_MODE",
		"VALKEY_ADDR", "VALKEY_PASSWORD", "VALKEY_DIAL_TIMEOUT", "VALKEY_READ_TIMEOUT", "VALKEY_WRITE_TIMEOUT", "IDEMPOTENCY_TTL",
		"CONNECTOR_STREAM_KEY", "CONNECTOR_STREAM_GROUP", "CONNECTOR_STREAM_CONSUMER_NAME", "CONNECTOR_STREAM_BLOCK_TIMEOUT",
		"CONNECTOR_STREAM_CLAIM_MIN_IDLE", "CONNECTOR_STREAM_BATCH_SIZE", "CONNECTOR_STREAM_MAX_DELIVERIES",
		"CONNECTOR_STREAM_DEAD_LETTER_KEY", "OPENBAO_ADDR", "OPENBAO_TOKEN", "OPENBAO_MOUNT", "OPENBAO_TIMEOUT",
		"CONNECTOR_ALIAS_FETCH_TIMEOUT", "CONNECTOR_ALIAS_REFRESH_INTERVAL",
		"CONNECTOR_POOL_SIZE_STORAGE", "CONNECTOR_POOL_SIZE_SEND_EMAIL", "CONNECTOR_POOL_SIZE_DOCUMENT_EXTRACT",
		"CONNECTOR_POOL_SIZE_REST_CALL", "CONNECTOR_POOL_SIZE_SQL_QUERY", "CONNECTOR_POOL_SIZE_CHAT_NOTIFY",
		"CONNECTOR_TIMEOUT_STORAGE", "CONNECTOR_TIMEOUT_SEND_EMAIL", "CONNECTOR_TIMEOUT_DOCUMENT_EXTRACT",
		"CONNECTOR_TIMEOUT_REST_CALL", "CONNECTOR_TIMEOUT_SQL_QUERY", "CONNECTOR_TIMEOUT_CHAT_NOTIFY",
		"CONNECTOR_COMPLETION_TIMEOUT", "CONNECTOR_RETRY_MAX_ATTEMPTS", "CONNECTOR_RETRY_INITIAL_BACKOFF",
		"CONNECTOR_RETRY_MAX_BACKOFF", "CONNECTOR_AUDIT_RETENTION",
		"OUTBOX_RELAY_DATABASE_URL", "LOCK_DATABASE_URL", "MIGRATION_DATABASE_URL",
	} {
		t.Setenv(key, "")
	}
}

type w10aDefault struct {
	name string
	got  any
	want any
}

func w10aRequireDefaults(t *testing.T, checks []w10aDefault) {
	t.Helper()
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %v (%T), want %v (%T)", c.got, c.got, c.want, c.want)
			}
		})
	}
}

func TestLoad_CleanEnvironment_DatabaseAndValkeyDefaults(t *testing.T) {
	w10aCleanEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil: no setting is required to load", err)
	}

	w10aRequireDefaults(t, []w10aDefault{
		{"database url is empty", cfg.DatabaseURL, ""},
		{"max connections", cfg.PGMaxConns, int32(10)},
		{"min connections", cfg.PGMinConns, int32(2)},
		{"slow query threshold in ms", cfg.PGSlowQueryThresholdMS, 200},
		{"bouncer mode is off", cfg.PGBouncerMode, false},
		{"valkey address", cfg.ValkeyAddr, "localhost:6379"},
		{"valkey password is empty", cfg.ValkeyPassword, ""},
		{"valkey dial timeout", cfg.ValkeyDialTimeout, 2 * time.Second},
		{"valkey read timeout", cfg.ValkeyReadTimeout, time.Second},
		{"valkey write timeout", cfg.ValkeyWriteTimeout, time.Second},
		{"idempotency ttl", cfg.IdempotencyTTL, 24 * time.Hour},
	})
}

func TestLoad_CleanEnvironment_ConnectorStreamOpenBaoAndAliasDefaults(t *testing.T) {
	w10aCleanEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil: no setting is required to load", err)
	}

	w10aRequireDefaults(t, []w10aDefault{
		{"stream key", cfg.ConnectorStreamKey, "connector-tasks"},
		{"stream group", cfg.ConnectorStreamGroup, "connector-worker"},
		{"consumer name is empty", cfg.ConnectorStreamConsumerName, ""},
		{"block timeout", cfg.ConnectorStreamBlockTimeout, 5 * time.Second},
		{"claim min idle", cfg.ConnectorStreamClaimMinIdle, 30 * time.Second},
		{"batch size", cfg.ConnectorStreamBatchSize, int64(10)},
		{"max deliveries", cfg.ConnectorStreamMaxDeliveries, int64(10)},
		{"dead letter key", cfg.ConnectorStreamDeadLetterKey, "connector-tasks-dead"},
		{"openbao address is empty", cfg.OpenBaoAddr, ""},
		{"openbao token is empty", cfg.OpenBaoToken, ""},
		{"openbao mount", cfg.OpenBaoMount, "secret"},
		{"openbao timeout", cfg.OpenBaoTimeout, 5 * time.Second},
		{"alias fetch timeout", cfg.ConnectorAliasFetchTimeout, 10 * time.Second},
		{"alias refresh interval", cfg.ConnectorAliasRefreshInterval, 5 * time.Minute},
	})
}

func TestLoad_CleanEnvironment_ConnectorPoolTimeoutCompletionAndRetryDefaults(t *testing.T) {
	w10aCleanEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil: no setting is required to load", err)
	}

	w10aRequireDefaults(t, []w10aDefault{
		{"storage pool size", cfg.ConnectorPoolSizeStorage, 10},
		{"send email pool size", cfg.ConnectorPoolSizeSendEmail, 5},
		{"document extract pool size", cfg.ConnectorPoolSizeDocumentExtract, 10},
		{"rest call pool size", cfg.ConnectorPoolSizeRestCall, 20},
		{"sql query pool size", cfg.ConnectorPoolSizeSQLQuery, 15},
		{"chat notify pool size", cfg.ConnectorPoolSizeChatNotify, 5},
		{"storage timeout", cfg.ConnectorTimeoutStorage, 60 * time.Second},
		{"send email timeout", cfg.ConnectorTimeoutSendEmail, 10 * time.Second},
		{"document extract timeout", cfg.ConnectorTimeoutDocumentExtract, 60 * time.Second},
		{"rest call timeout", cfg.ConnectorTimeoutRestCall, 30 * time.Second},
		{"sql query timeout", cfg.ConnectorTimeoutSQLQuery, 15 * time.Second},
		{"chat notify timeout", cfg.ConnectorTimeoutChatNotify, 10 * time.Second},
		{"completion timeout", cfg.ConnectorCompletionTimeout, 5 * time.Second},
		{"retry max attempts", cfg.ConnectorRetryMaxAttempts, 5},
		{"retry initial backoff", cfg.ConnectorRetryInitialBackoff, 500 * time.Millisecond},
		{"retry max backoff", cfg.ConnectorRetryMaxBackoff, 10 * time.Second},
		{"audit retention", cfg.ConnectorAuditRetention, 2160 * time.Hour},
	})
}

func TestLoad_Secrets_AreReadFromTheEnvironmentOnly(t *testing.T) {
	cases := []struct {
		key   string
		field func(*Config) string
	}{
		{"DATABASE_URL", func(c *Config) string { return c.DatabaseURL }},
		{"OUTBOX_RELAY_DATABASE_URL", func(c *Config) string { return c.OutboxRelayDatabaseURL }},
		{"LOCK_DATABASE_URL", func(c *Config) string { return c.LockDatabaseURL }},
		{"MIGRATION_DATABASE_URL", func(c *Config) string { return c.MigrationDatabaseURL }},
		{"VALKEY_PASSWORD", func(c *Config) string { return c.ValkeyPassword }},
		{"OPENBAO_TOKEN", func(c *Config) string { return c.OpenBaoToken }},
	}
	for _, tc := range cases {
		t.Run(tc.key+" from the variable", func(t *testing.T) {
			w10aCleanEnv(t)
			t.Setenv(tc.key, "value-from-the-environment")

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}

			if got := tc.field(cfg); got != "value-from-the-environment" {
				t.Errorf("%s = %q, want the variable's value", tc.key, got)
			}
		})
		t.Run(tc.key+" has no file variant", func(t *testing.T) {
			w10aCleanEnv(t)
			secretFile := filepath.Join(t.TempDir(), "secret")
			if err := os.WriteFile(secretFile, []byte("value-from-the-file"), 0o600); err != nil {
				t.Fatalf("write secret file: %v", err)
			}
			t.Setenv(tc.key+"_FILE", secretFile)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}

			if got := tc.field(cfg); got != "" {
				t.Errorf("%s = %q, want empty: a %s_FILE variable is not read", tc.key, got, tc.key)
			}
		})
	}
}

func TestVersionRequested_ReadsTheFirstArgument(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"version as the first argument", []string{"server", "version"}, true},
		{"no arguments", []string{"server"}, false},
		{"another command", []string{"server", "migrate"}, false},
		{"capitalised word", []string{"server", "Version"}, false},
		{"version after another word", []string{"server", "serve", "version"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			saved := os.Args
			t.Cleanup(func() { os.Args = saved })
			os.Args = tc.args

			if got := VersionRequested(); got != tc.want {
				t.Errorf("VersionRequested() with %v = %v, want %v", tc.args[1:], got, tc.want)
			}
		})
	}
}
