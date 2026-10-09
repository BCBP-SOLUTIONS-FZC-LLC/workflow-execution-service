package config

import (
	"strings"
	"testing"
	"time"
)

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "dev")
	t.Setenv("DEFINITION_SERVICE_URL", "http://definition:8080")
	t.Setenv("DATABASE_URL", "postgres://wfexec:wfexec@localhost:5432/workflow_execution?sslmode=disable")
	t.Setenv("HTTP_PORT", "8080")
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
		{"definition client timeout zero", func(c *Config) { c.DefinitionClientTimeout = 0 }, true},
		{"eligibility client timeout zero", func(c *Config) { c.EligibilityClientTimeout = 0 }, true},
		{"iam client timeout zero", func(c *Config) { c.IAMClientTimeout = 0 }, true},
		{"token service client timeout zero", func(c *Config) { c.TokenServiceClientTimeout = 0 }, true},
		{"app env unknown", func(c *Config) { c.AppEnv = "production" }, true},
		{"app env empty", func(c *Config) { c.AppEnv = "" }, true},
		{"app env local", func(c *Config) { c.AppEnv = "local" }, false},
		{"app env staging", func(c *Config) { c.AppEnv = "staging" }, false},
		{"rls violation export interval zero", func(c *Config) { c.RLSViolationExportInterval = 0 }, true},
		{"rls violation retention zero", func(c *Config) { c.RLSViolationRetention = 0 }, true},
		{"operational stats interval zero", func(c *Config) { c.OperationalStatsInterval = 0 }, true},
		{"queue topology poll interval zero", func(c *Config) { c.QueueTopologyPollInterval = 0 }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				AppEnv:                     "dev",
				DatabaseURL:                "postgres://x",
				HTTPPort:                   8080,
				MetricsPort:                9091,
				WorkerHealthPort:           8081,
				WorkerMetricsPort:          8082,
				PGMaxConns:                 10,
				PGMinConns:                 2,
				OTELTracesSamplerRatio:     1.0,
				OutboxBatchSize:            50,
				OutboxPollInterval:         time.Second,
				DefinitionClientTimeout:    5 * time.Second,
				EligibilityClientTimeout:   5 * time.Second,
				IAMClientTimeout:           5 * time.Second,
				QueueTopologyPollInterval:  60 * time.Second,
				TokenServiceClientTimeout:  5 * time.Second,
				RLSViolationExportInterval: 30 * time.Second,
				RLSViolationRetention:      time.Hour,
				OperationalStatsInterval:   30 * time.Second,
			}
			tt.mutate(cfg)

			err := cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoad_DefaultServiceName(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.OTELServiceName != "workflow-execution-service" {
		t.Errorf("OTELServiceName = %q, want workflow-execution-service", cfg.OTELServiceName)
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

// A binary needs only the settings it uses: the connector-worker has no
// database and publishes no events, and the migrate job needs only the DSN.
func TestLoad_ConnectorWorkerEnvironment(t *testing.T) {
	t.Setenv("APP_ENV", "prod")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OUTBOX_RELAY_DATABASE_URL", "")
	t.Setenv("AWS_USE_STUB", "false")
	t.Setenv("SNS_TOPIC_ARN", "")
	t.Setenv("GLUE_REGISTRY_NAME", "")
	t.Setenv("DEFINITION_SERVICE_URL", "http://definition:8080")
	t.Setenv("DEFINITION_SERVICE_TOKEN", "def-tok")
	t.Setenv("INTERNAL_API_TOKEN", "tok")

	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, want nil for the connector-worker's environment", err)
	}
}

func TestRequire(t *testing.T) {
	prod := func(c *Config) {
		c.AppEnv = "prod"
		c.InternalAPIToken = "tok"
		c.DefinitionServiceToken = "def-tok"
		c.OutboxRelayDatabaseURL = "postgres://relay"
		c.TokenServiceBaseURL = "http://token-service"
		c.AWSUseStub = false
		c.SNSTopicARN = "arn:aws:sns:x"
		c.GlueRegistryName = "wf-workflow-events"
	}
	base := func() *Config {
		return &Config{
			AppEnv:               "dev",
			DatabaseURL:          "postgres://x",
			DefinitionServiceURL: "http://definition:8080",
			EligibilityBaseURL:   "http://eligibility",
			AWSUseStub:           true,
		}
	}
	tests := []struct {
		name    string
		require func(*Config) error
		mutate  func(*Config)
		wantErr string
	}{
		{"server valid in dev", (*Config).RequireServer, func(c *Config) {}, ""},
		{"server valid in production", (*Config).RequireServer, prod, ""},
		{"server needs a database", (*Config).RequireServer, func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL"},
		{"server needs the definition service", (*Config).RequireServer, func(c *Config) { c.DefinitionServiceURL = "" }, "DEFINITION_SERVICE_URL"},
		{"server needs the eligibility service", (*Config).RequireServer, func(c *Config) { c.EligibilityBaseURL = "" }, "ELIGIBILITY_BASE_URL"},
		{"server needs sns when not stubbed", (*Config).RequireServer, func(c *Config) { c.AWSUseStub = false; c.GlueRegistryName = "g" }, "SNS_TOPIC_ARN"},
		{"server needs glue when not stubbed", (*Config).RequireServer, func(c *Config) { c.AWSUseStub = false; c.SNSTopicARN = "arn" }, "GLUE_REGISTRY_NAME"},
		{"server refuses the stubbed publisher outside dev", (*Config).RequireServer, func(c *Config) { prod(c); c.AWSUseStub = true }, "AWS_USE_STUB"},
		{"server allows the stubbed publisher in local", (*Config).RequireServer, func(c *Config) { c.AppEnv = "local" }, ""},
		{"server needs its token outside dev", (*Config).RequireServer, func(c *Config) { prod(c); c.InternalAPIToken = "" }, "INTERNAL_API_TOKEN"},
		{"server needs the definition token outside dev", (*Config).RequireServer, func(c *Config) { prod(c); c.DefinitionServiceToken = "" }, "DEFINITION_SERVICE_TOKEN"},
		{"server needs the token service outside dev", (*Config).RequireServer, func(c *Config) { prod(c); c.TokenServiceBaseURL = "" }, "TOKEN_SERVICE_BASE_URL"},
		{"server needs the relay dsn outside dev", (*Config).RequireServer, func(c *Config) { prod(c); c.OutboxRelayDatabaseURL = "" }, "OUTBOX_RELAY_DATABASE_URL"},

		{"worker valid in dev", (*Config).RequireWorker, func(c *Config) {}, ""},
		{"worker valid in production without the api's settings", (*Config).RequireWorker, func(c *Config) { c.AppEnv = "prod"; c.DefinitionServiceToken = "def-tok" }, ""},
		{"worker needs a database", (*Config).RequireWorker, func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL"},
		{"worker needs the definition service", (*Config).RequireWorker, func(c *Config) { c.DefinitionServiceURL = "" }, "DEFINITION_SERVICE_URL"},
		{"worker needs the definition token outside dev", (*Config).RequireWorker, func(c *Config) { c.AppEnv = "prod" }, "DEFINITION_SERVICE_TOKEN"},

		{"connector-worker valid in production without a database", (*Config).RequireConnectorWorker, func(c *Config) {
			c.AppEnv = "prod"
			c.DatabaseURL = ""
			c.InternalAPIToken = "tok"
			c.DefinitionServiceToken = "def-tok"
		}, ""},
		{"connector-worker needs the definition service", (*Config).RequireConnectorWorker, func(c *Config) { c.DefinitionServiceURL = "" }, "DEFINITION_SERVICE_URL"},
		{"connector-worker needs the definition token outside dev", (*Config).RequireConnectorWorker, func(c *Config) { c.AppEnv = "prod"; c.InternalAPIToken = "tok" }, "DEFINITION_SERVICE_TOKEN"},
		{"connector-worker needs the internal token outside dev", (*Config).RequireConnectorWorker, func(c *Config) { c.AppEnv = "prod"; c.DefinitionServiceToken = "def-tok" }, "INTERNAL_API_TOKEN"},

		{"migrate needs only a database", (*Config).RequireMigrate, func(c *Config) { *c = Config{DatabaseURL: "postgres://x"} }, ""},
		{"migrate prefers its direct dsn", (*Config).RequireMigrate, func(c *Config) { *c = Config{MigrationDatabaseURL: "postgres://direct"} }, ""},
		{"migrate needs a database", (*Config).RequireMigrate, func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			tt.mutate(cfg)
			err := tt.require(cfg)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("error = %v, want nil", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("error = %v, want one naming %s", err, tt.wantErr)
			}
		})
	}
}
