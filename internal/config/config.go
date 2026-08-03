package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv       string
	BuildVersion string

	HTTPPort         int
	GRPCPort         int
	WorkerHealthPort int

	// OTel — consumed by platform-gincommon.InitTracingFromEnv()
	OTELServiceName        string
	OTELExporterEndpoint   string
	OTELExporterInsecure   bool
	OTELTracesSamplerRatio float64

	DatabaseURL            string
	PGMaxConns             int32
	PGMinConns             int32
	PGSlowQueryThresholdMS int
	PGBouncerMode          bool
	// MigrationDatabaseURL overrides DatabaseURL for the migrate subcommand.
	// Set to a direct Postgres DSN when DATABASE_URL points to PgBouncer —
	// golang-migrate uses session advisory locks that PgBouncer transaction
	// pooling drops. Falls back to DatabaseURL when unset.
	MigrationDatabaseURL string

	ValkeyAddr        string
	ValkeyPassword    string
	ValkeyDialTimeout time.Duration
	ValkeyReadTimeout time.Duration
	IdempotencyTTL    time.Duration

	TemporalHostPort  string
	TemporalNamespace string
	TemporalTaskQueue string

	AWSUseStub     bool
	AWSRegion      string
	SNSTopicARN    string
	AWSEndpointURL string

	OutboxPollInterval time.Duration
	OutboxBatchSize    int

	DefinitionServiceAddr string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		AppEnv:       getEnvOrDefault("APP_ENV", "dev"),
		BuildVersion: getEnvOrDefault("BUILD_VERSION", "dev"),

		HTTPPort:         getEnvIntOrDefault("HTTP_PORT", 8080),
		GRPCPort:         getEnvIntOrDefault("GRPC_PORT", 9090),
		WorkerHealthPort: getEnvIntOrDefault("WORKER_HEALTH_PORT", 8081),

		OTELServiceName:        getEnvOrDefault("OTEL_SERVICE_NAME", "execution-service"),
		OTELExporterEndpoint:   getEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		OTELExporterInsecure:   getEnvBoolOrDefault("OTEL_EXPORTER_OTLP_INSECURE", true),
		OTELTracesSamplerRatio: getEnvFloat64OrDefault("OTEL_TRACES_SAMPLER_RATIO", 1.0),

		DatabaseURL:            getEnvOrDefault("DATABASE_URL", ""),
		PGMaxConns:             int32(getEnvIntOrDefault("PG_MAX_CONNS", 10)),
		PGMinConns:             int32(getEnvIntOrDefault("PG_MIN_CONNS", 2)),
		PGSlowQueryThresholdMS: getEnvIntOrDefault("PG_SLOW_QUERY_THRESHOLD_MS", 200),
		PGBouncerMode:          getEnvBoolOrDefault("PG_BOUNCER_MODE", false),
		MigrationDatabaseURL:   getEnvOrDefault("MIGRATION_DATABASE_URL", ""),

		ValkeyAddr:        getEnvOrDefault("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword:    getEnvOrDefault("VALKEY_PASSWORD", ""),
		ValkeyDialTimeout: getEnvDurationOrDefault("VALKEY_DIAL_TIMEOUT", 2*time.Second),
		ValkeyReadTimeout: getEnvDurationOrDefault("VALKEY_READ_TIMEOUT", 1*time.Second),
		IdempotencyTTL:    getEnvDurationOrDefault("IDEMPOTENCY_TTL", 24*time.Hour),

		TemporalHostPort:  getEnvOrDefault("TEMPORAL_HOST_PORT", "localhost:7233"),
		TemporalNamespace: getEnvOrDefault("TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueue: getEnvOrDefault("TEMPORAL_TASK_QUEUE", "execution-default"),

		AWSUseStub:     getEnvBoolOrDefault("AWS_USE_STUB", true),
		AWSRegion:      getEnvOrDefault("AWS_REGION", "us-east-1"),
		SNSTopicARN:    getEnvOrDefault("SNS_TOPIC_ARN", ""),
		AWSEndpointURL: getEnvOrDefault("AWS_ENDPOINT_URL", ""),

		OutboxPollInterval: getEnvDurationOrDefault("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:    getEnvIntOrDefault("OUTBOX_BATCH_SIZE", 50),

		DefinitionServiceAddr: getEnvOrDefault("DEFINITION_SERVICE_ADDR", ""),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("HTTP_PORT must be in [1, 65535]")
	}
	if c.GRPCPort < 1 || c.GRPCPort > 65535 {
		return fmt.Errorf("GRPC_PORT must be in [1, 65535]")
	}
	if c.WorkerHealthPort < 1 || c.WorkerHealthPort > 65535 {
		return fmt.Errorf("WORKER_HEALTH_PORT must be in [1, 65535]")
	}
	if c.PGMaxConns <= 0 {
		return fmt.Errorf("PG_MAX_CONNS must be > 0")
	}
	if c.PGMinConns < 0 {
		return fmt.Errorf("PG_MIN_CONNS must be >= 0")
	}
	if c.PGMinConns > c.PGMaxConns {
		return fmt.Errorf("PG_MIN_CONNS must be <= PG_MAX_CONNS")
	}
	if c.OTELTracesSamplerRatio < 0.0 || c.OTELTracesSamplerRatio > 1.0 {
		return fmt.Errorf("OTEL_TRACES_SAMPLER_RATIO must be in [0.0, 1.0]")
	}
	if c.OutboxBatchSize <= 0 {
		return fmt.Errorf("OUTBOX_BATCH_SIZE must be > 0")
	}
	if c.OutboxPollInterval <= 0 {
		return fmt.Errorf("OUTBOX_POLL_INTERVAL must be > 0")
	}
	if !c.AWSUseStub {
		if c.SNSTopicARN == "" {
			return fmt.Errorf("SNS_TOPIC_ARN is required when AWS_USE_STUB=false")
		}
	}
	return nil
}

// MigrationDSN returns the DSN to use for schema migrations. It prefers
// MIGRATION_DATABASE_URL so migrations can bypass PgBouncer (advisory locks
// require a direct Postgres connection), falling back to DATABASE_URL.
func (c *Config) MigrationDSN() string {
	if c.MigrationDatabaseURL != "" {
		return c.MigrationDatabaseURL
	}
	return c.DatabaseURL
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvIntOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvBoolOrDefault(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvFloat64OrDefault(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func getEnvDurationOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
