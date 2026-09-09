package config

import (
	"testing"
	"time"
)

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "dev")
	t.Setenv("DATABASE_URL", "postgres://wfexec:wfexec@localhost:5432/workflow_execution?sslmode=disable")
	t.Setenv("HTTP_PORT", "8080")
	t.Setenv("GRPC_PORT", "9090")
	t.Setenv("METRICS_PORT", "9091")
	t.Setenv("WORKER_HEALTH_PORT", "8081")
	t.Setenv("WORKER_METRICS_PORT", "8082")
	t.Setenv("PG_MAX_CONNS", "10")
	t.Setenv("PG_MIN_CONNS", "2")
	t.Setenv("OTEL_TRACES_SAMPLER_RATIO", "1.0")
	t.Setenv("OUTBOX_BATCH_SIZE", "50")
	t.Setenv("OUTBOX_POLL_INTERVAL", "500ms")
	t.Setenv("AWS_USE_STUB", "true")
	t.Setenv("SNS_TOPIC_ARN", "")
}

func TestLoad_Success(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.DatabaseURL == "" {
		t.Error("DatabaseURL should not be empty")
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want 8080", cfg.HTTPPort)
	}
}

func TestLoad_MissingDatabaseURL(t *testing.T) {
	setValidEnv(t)
	t.Setenv("DATABASE_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for missing DATABASE_URL")
	}
}

func TestLoad_InvalidIntEnvVarFailsFast(t *testing.T) {
	setValidEnv(t)
	t.Setenv("HTTP_PORT", "not-a-port")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error for a malformed HTTP_PORT rather than silently booting on its default")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(c *Config) {}, false},
		{"http port too low", func(c *Config) { c.HTTPPort = 0 }, true},
		{"http port too high", func(c *Config) { c.HTTPPort = 70000 }, true},
		{"grpc port too low", func(c *Config) { c.GRPCPort = 0 }, true},
		{"metrics port too low", func(c *Config) { c.MetricsPort = 0 }, true},
		{"worker health port too low", func(c *Config) { c.WorkerHealthPort = 0 }, true},
		{"worker metrics port too low", func(c *Config) { c.WorkerMetricsPort = 0 }, true},
		{"pg max conns zero", func(c *Config) { c.PGMaxConns = 0 }, true},
		{"pg min conns negative", func(c *Config) { c.PGMinConns = -1 }, true},
		{"pg min exceeds max", func(c *Config) { c.PGMinConns = 20; c.PGMaxConns = 10 }, true},
		{"sampler ratio too high", func(c *Config) { c.OTELTracesSamplerRatio = 1.5 }, true},
		{"sampler ratio negative", func(c *Config) { c.OTELTracesSamplerRatio = -0.1 }, true},
		{"outbox batch size zero", func(c *Config) { c.OutboxBatchSize = 0 }, true},
		{"outbox poll interval zero", func(c *Config) { c.OutboxPollInterval = 0 }, true},
		{"aws not stubbed without sns topic", func(c *Config) { c.AWSUseStub = false; c.SNSTopicARN = "" }, true},
		{"aws not stubbed with sns topic", func(c *Config) { c.AWSUseStub = false; c.SNSTopicARN = "arn:aws:sns:x" }, false},
		{"definition client timeout zero", func(c *Config) { c.DefinitionClientTimeout = 0 }, true},
		{"eligibility client timeout zero", func(c *Config) { c.EligibilityClientTimeout = 0 }, true},
		{"iam client timeout zero", func(c *Config) { c.IAMClientTimeout = 0 }, true},
		{"queue topology poll interval zero", func(c *Config) { c.QueueTopologyPollInterval = 0 }, true},
		{"internal api token required in prod", func(c *Config) { c.AppEnv = "prod" }, true},
		{"internal api token required outside dev even when not prod", func(c *Config) {
			c.AppEnv = "staging"
			c.OutboxRelayDatabaseURL = "postgres://relay"
		}, true},
		{"internal api token set in prod", func(c *Config) {
			c.AppEnv = "prod"
			c.InternalAPIToken = "tok"
			c.OutboxRelayDatabaseURL = "postgres://relay"
		}, false},
		{"outbox relay database url required outside dev", func(c *Config) { c.AppEnv = "staging"; c.InternalAPIToken = "tok" }, true},
		{"outbox relay database url set outside dev", func(c *Config) {
			c.AppEnv = "staging"
			c.InternalAPIToken = "tok"
			c.OutboxRelayDatabaseURL = "postgres://relay"
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				AppEnv:                    "dev",
				DatabaseURL:               "postgres://x",
				HTTPPort:                  8080,
				GRPCPort:                  9090,
				MetricsPort:               9091,
				WorkerHealthPort:          8081,
				WorkerMetricsPort:         8082,
				PGMaxConns:                10,
				PGMinConns:                2,
				OTELTracesSamplerRatio:    1.0,
				OutboxBatchSize:           50,
				OutboxPollInterval:        time.Second,
				AWSUseStub:                true,
				GlueRegistryName:          "wf-workflow-events",
				DefinitionClientTimeout:   5 * time.Second,
				EligibilityClientTimeout:  5 * time.Second,
				IAMClientTimeout:          5 * time.Second,
				QueueTopologyPollInterval: 60 * time.Second,
			}
			tt.mutate(cfg)

			err := cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMigrationDSN(t *testing.T) {
	cfg := &Config{DatabaseURL: "postgres://primary"}
	if got := cfg.MigrationDSN(); got != "postgres://primary" {
		t.Errorf("MigrationDSN() = %q, want fallback to DatabaseURL", got)
	}

	cfg.MigrationDatabaseURL = "postgres://direct"
	if got := cfg.MigrationDSN(); got != "postgres://direct" {
		t.Errorf("MigrationDSN() = %q, want MigrationDatabaseURL", got)
	}
}

func TestOutboxRelayDSN(t *testing.T) {
	cfg := &Config{DatabaseURL: "postgres://primary"}
	if got := cfg.OutboxRelayDSN(); got != "postgres://primary" {
		t.Errorf("OutboxRelayDSN() = %q, want fallback to DatabaseURL", got)
	}

	cfg.OutboxRelayDatabaseURL = "postgres://relay"
	if got := cfg.OutboxRelayDSN(); got != "postgres://relay" {
		t.Errorf("OutboxRelayDSN() = %q, want OutboxRelayDatabaseURL", got)
	}
}

func TestGetEnvOrDefault(t *testing.T) {
	t.Setenv("TEST_STR_VAR", "value")
	if got := getEnvOrDefault("TEST_STR_VAR", "fallback"); got != "value" {
		t.Errorf("getEnvOrDefault() = %q, want %q", got, "value")
	}
	if got := getEnvOrDefault("TEST_STR_VAR_UNSET", "fallback"); got != "fallback" {
		t.Errorf("getEnvOrDefault() = %q, want fallback", got)
	}
}

func TestGetEnvIntOrDefault(t *testing.T) {
	t.Setenv("TEST_INT_VAR", "42")
	if got, err := getEnvIntOrDefault("TEST_INT_VAR", 1); got != 42 || err != nil {
		t.Errorf("getEnvIntOrDefault() = (%d, %v), want (42, nil)", got, err)
	}
	t.Setenv("TEST_INT_VAR_BAD", "not-an-int")
	if got, err := getEnvIntOrDefault("TEST_INT_VAR_BAD", 7); got != 7 || err == nil {
		t.Errorf("getEnvIntOrDefault() = (%d, %v), want (fallback 7, a parse error)", got, err)
	}
	if got, err := getEnvIntOrDefault("TEST_INT_VAR_UNSET", 9); got != 9 || err != nil {
		t.Errorf("getEnvIntOrDefault() = (%d, %v), want (9, nil)", got, err)
	}
}

func TestGetEnvBoolOrDefault(t *testing.T) {
	t.Setenv("TEST_BOOL_VAR", "false")
	if got, err := getEnvBoolOrDefault("TEST_BOOL_VAR", true); got || err != nil {
		t.Errorf("getEnvBoolOrDefault() = (%v, %v), want (false, nil)", got, err)
	}
	t.Setenv("TEST_BOOL_VAR_BAD", "not-a-bool")
	if got, err := getEnvBoolOrDefault("TEST_BOOL_VAR_BAD", true); !got || err == nil {
		t.Errorf("getEnvBoolOrDefault() = (%v, %v), want (fallback true, a parse error)", got, err)
	}
	if got, err := getEnvBoolOrDefault("TEST_BOOL_VAR_UNSET", true); !got || err != nil {
		t.Errorf("getEnvBoolOrDefault() = (%v, %v), want (true, nil)", got, err)
	}
}

func TestGetEnvFloat64OrDefault(t *testing.T) {
	t.Setenv("TEST_FLOAT_VAR", "0.5")
	if got, err := getEnvFloat64OrDefault("TEST_FLOAT_VAR", 1.0); got != 0.5 || err != nil {
		t.Errorf("getEnvFloat64OrDefault() = (%v, %v), want (0.5, nil)", got, err)
	}
	t.Setenv("TEST_FLOAT_VAR_BAD", "not-a-float")
	if got, err := getEnvFloat64OrDefault("TEST_FLOAT_VAR_BAD", 1.5); got != 1.5 || err == nil {
		t.Errorf("getEnvFloat64OrDefault() = (%v, %v), want (fallback 1.5, a parse error)", got, err)
	}
	if got, err := getEnvFloat64OrDefault("TEST_FLOAT_VAR_UNSET", 2.5); got != 2.5 || err != nil {
		t.Errorf("getEnvFloat64OrDefault() = (%v, %v), want (2.5, nil)", got, err)
	}
}

func TestGetEnvDurationOrDefault(t *testing.T) {
	t.Setenv("TEST_DURATION_VAR", "2s")
	if got, err := getEnvDurationOrDefault("TEST_DURATION_VAR", time.Second); got != 2*time.Second || err != nil {
		t.Errorf("getEnvDurationOrDefault() = (%v, %v), want (2s, nil)", got, err)
	}
	t.Setenv("TEST_DURATION_VAR_BAD", "not-a-duration")
	want := 30 * time.Second
	if got, err := getEnvDurationOrDefault("TEST_DURATION_VAR_BAD", want); got != want || err == nil {
		t.Errorf("getEnvDurationOrDefault() = (%v, %v), want (fallback %v, a parse error)", got, err, want)
	}
	if got, err := getEnvDurationOrDefault("TEST_DURATION_VAR_UNSET", want); got != want || err != nil {
		t.Errorf("getEnvDurationOrDefault() = (%v, %v), want (%v, nil)", got, err, want)
	}
}
