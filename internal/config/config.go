package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv       string
	BuildVersion string

	HTTPPort          int
	MetricsPort       int
	WorkerHealthPort  int
	WorkerMetricsPort int

	OTELServiceName        string
	OTELExporterEndpoint   string
	OTELExporterInsecure   bool
	OTELTracesSamplerRatio float64

	DatabaseURL            string
	PGMaxConns             int32
	PGMinConns             int32
	PGSlowQueryThresholdMS int
	PGBouncerMode          bool
	MigrationDatabaseURL   string

	ValkeyAddr         string
	ValkeyPassword     string
	ValkeyDialTimeout  time.Duration
	ValkeyReadTimeout  time.Duration
	ValkeyWriteTimeout time.Duration
	IdempotencyTTL     time.Duration

	TemporalHostPort  string
	TemporalNamespace string
	TemporalTaskQueue string

	AWSUseStub     bool
	AWSRegion      string
	SNSTopicARN    string
	AWSEndpointURL string

	OutboxPollInterval time.Duration

	RLSViolationExportInterval time.Duration
	RLSViolationRetention      time.Duration
	OutboxRetention            time.Duration
	TenantPurgeEnabled         bool
	TenantHistoryRetention     time.Duration
	ProcessedEventRetention    time.Duration
	RetentionSweepInterval     time.Duration
	RetentionSweepBatch        int
	OperationalStatsInterval   time.Duration
	OutboxBatchSize            int

	OutboxRelayDatabaseURL string

	DefinitionServiceURL    string
	DefinitionServiceToken  string
	DefinitionClientTimeout time.Duration

	EligibilityBaseURL       string
	EligibilityClientTimeout time.Duration

	IAMBaseURL       string
	IAMClientTimeout time.Duration

	TokenServiceBaseURL       string
	TokenServiceClientTimeout time.Duration

	QueueTopologyPollInterval time.Duration

	InternalAPIToken string

	GlueRegistryName   string
	GlueSchemaCacheTTL time.Duration

	ConnectorStreamKey           string
	ConnectorStreamGroup         string
	ConnectorStreamConsumerName  string
	ConnectorStreamBlockTimeout  time.Duration
	ConnectorStreamClaimMinIdle  time.Duration
	ConnectorStreamBatchSize     int64
	ConnectorStreamMaxDeliveries int64
	ConnectorStreamDeadLetterKey string

	OpenBaoAddr    string
	OpenBaoToken   string
	OpenBaoMount   string
	OpenBaoTimeout time.Duration

	ConnectorAliasFetchTimeout    time.Duration
	ConnectorAliasRefreshInterval time.Duration

	ConnectorPoolSizeStorage         int
	ConnectorPoolSizeSendEmail       int
	ConnectorPoolSizeDocumentExtract int
	ConnectorPoolSizeRestCall        int
	ConnectorPoolSizeSQLQuery        int
	ConnectorPoolSizeChatNotify      int
	ConnectorTimeoutStorage          time.Duration
	ConnectorTimeoutSendEmail        time.Duration
	ConnectorTimeoutDocumentExtract  time.Duration
	ConnectorTimeoutRestCall         time.Duration
	ConnectorTimeoutSQLQuery         time.Duration
	ConnectorTimeoutChatNotify       time.Duration

	ExecutionServiceInternalAddr string
	ConnectorCompletionTimeout   time.Duration

	ConnectorDocsBucket          string
	ConnectorRetryMaxAttempts    int
	ConnectorRetryInitialBackoff time.Duration
	ConnectorRetryMaxBackoff     time.Duration
	ConnectorAuditRetention      time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	var parseErrs []string
	record := func(err error) {
		if err != nil {
			parseErrs = append(parseErrs, err.Error())
		}
	}
	envInt := func(key string, fallback int) int {
		v, err := getEnvIntOrDefault(key, fallback)
		record(err)
		return v
	}
	envBool := func(key string, fallback bool) bool {
		v, err := getEnvBoolOrDefault(key, fallback)
		record(err)
		return v
	}
	envFloat64 := func(key string, fallback float64) float64 {
		v, err := getEnvFloat64OrDefault(key, fallback)
		record(err)
		return v
	}
	envDuration := func(key string, fallback time.Duration) time.Duration {
		v, err := getEnvDurationOrDefault(key, fallback)
		record(err)
		return v
	}

	cfg := &Config{
		AppEnv:       getEnvOrDefault("APP_ENV", "dev"),
		BuildVersion: getEnvOrDefault("BUILD_VERSION", "dev"),

		HTTPPort:          envInt("HTTP_PORT", 8080),
		MetricsPort:       envInt("METRICS_PORT", 9091),
		WorkerHealthPort:  envInt("WORKER_HEALTH_PORT", 8081),
		WorkerMetricsPort: envInt("WORKER_METRICS_PORT", 8082),

		OTELServiceName:        getEnvOrDefault("OTEL_SERVICE_NAME", "workflow-execution-service"),
		OTELExporterEndpoint:   getEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		OTELExporterInsecure:   envBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		OTELTracesSamplerRatio: envFloat64("OTEL_TRACES_SAMPLER_RATIO", 1.0),

		DatabaseURL:            getEnvOrDefault("DATABASE_URL", ""),
		PGMaxConns:             int32(envInt("PG_MAX_CONNS", 10)),
		PGMinConns:             int32(envInt("PG_MIN_CONNS", 2)),
		PGSlowQueryThresholdMS: envInt("PG_SLOW_QUERY_THRESHOLD_MS", 200),
		PGBouncerMode:          envBool("PG_BOUNCER_MODE", false),
		MigrationDatabaseURL:   getEnvOrDefault("MIGRATION_DATABASE_URL", ""),

		ValkeyAddr:         getEnvOrDefault("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword:     getEnvOrDefault("VALKEY_PASSWORD", ""),
		ValkeyDialTimeout:  envDuration("VALKEY_DIAL_TIMEOUT", 2*time.Second),
		ValkeyReadTimeout:  envDuration("VALKEY_READ_TIMEOUT", 1*time.Second),
		ValkeyWriteTimeout: envDuration("VALKEY_WRITE_TIMEOUT", 1*time.Second),
		IdempotencyTTL:     envDuration("IDEMPOTENCY_TTL", 24*time.Hour),

		TemporalHostPort:  getEnvOrDefault("TEMPORAL_HOST_PORT", "localhost:7233"),
		TemporalNamespace: getEnvOrDefault("TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueue: getEnvOrDefault("TEMPORAL_TASK_QUEUE", "wf-queue-default"),

		AWSUseStub:     envBool("AWS_USE_STUB", true),
		AWSRegion:      getEnvOrDefault("AWS_REGION", "us-east-1"),
		SNSTopicARN:    getEnvOrDefault("SNS_TOPIC_ARN", ""),
		AWSEndpointURL: getEnvOrDefault("AWS_ENDPOINT_URL", ""),

		OutboxPollInterval: envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),

		RLSViolationExportInterval: envDuration("RLS_VIOLATION_EXPORT_INTERVAL", 30*time.Second),
		RLSViolationRetention:      envDuration("RLS_VIOLATION_RETENTION", 7*24*time.Hour),
		OutboxRetention:            envDuration("OUTBOX_RETENTION", 7*365*24*time.Hour),
		TenantPurgeEnabled:         envBool("TENANT_PURGE_ENABLED", true),
		TenantHistoryRetention:     envDuration("TENANT_HISTORY_RETENTION", 7*365*24*time.Hour),
		ProcessedEventRetention:    envDuration("PROCESSED_EVENT_RETENTION", 7*24*time.Hour),
		RetentionSweepInterval:     envDuration("RETENTION_SWEEP_INTERVAL", 24*time.Hour),
		RetentionSweepBatch:        envInt("RETENTION_SWEEP_BATCH", 1000),
		OperationalStatsInterval:   envDuration("OPERATIONAL_STATS_INTERVAL", 30*time.Second),
		OutboxBatchSize:            envInt("OUTBOX_BATCH_SIZE", 50),
		OutboxRelayDatabaseURL:     getEnvOrDefault("OUTBOX_RELAY_DATABASE_URL", ""),

		DefinitionServiceURL:    getEnvOrDefault("DEFINITION_SERVICE_URL", ""),
		DefinitionServiceToken:  getEnvOrDefault("DEFINITION_SERVICE_TOKEN", ""),
		DefinitionClientTimeout: envDuration("DEFINITION_CLIENT_TIMEOUT", 5*time.Second),

		EligibilityBaseURL:       getEnvOrDefault("ELIGIBILITY_BASE_URL", ""),
		EligibilityClientTimeout: envDuration("ELIGIBILITY_CLIENT_TIMEOUT", 5*time.Second),

		IAMBaseURL:       getEnvOrDefault("IAM_BASE_URL", ""),
		IAMClientTimeout: envDuration("IAM_CLIENT_TIMEOUT", 5*time.Second),

		TokenServiceBaseURL:       getEnvOrDefault("TOKEN_SERVICE_BASE_URL", ""),
		TokenServiceClientTimeout: envDuration("TOKEN_SERVICE_CLIENT_TIMEOUT", 5*time.Second),

		QueueTopologyPollInterval: envDuration("QUEUE_TOPOLOGY_POLL_INTERVAL", 60*time.Second),

		InternalAPIToken: getEnvOrDefault("INTERNAL_API_TOKEN", ""),

		GlueRegistryName:   getEnvOrDefault("GLUE_REGISTRY_NAME", ""),
		GlueSchemaCacheTTL: envDuration("GLUE_SCHEMA_CACHE_TTL", 5*time.Minute),

		ConnectorStreamKey:           getEnvOrDefault("CONNECTOR_STREAM_KEY", "connector-tasks"),
		ConnectorStreamGroup:         getEnvOrDefault("CONNECTOR_STREAM_GROUP", "connector-worker"),
		ConnectorStreamConsumerName:  getEnvOrDefault("CONNECTOR_STREAM_CONSUMER_NAME", ""),
		ConnectorStreamBlockTimeout:  envDuration("CONNECTOR_STREAM_BLOCK_TIMEOUT", 5*time.Second),
		ConnectorStreamClaimMinIdle:  envDuration("CONNECTOR_STREAM_CLAIM_MIN_IDLE", 30*time.Second),
		ConnectorStreamBatchSize:     int64(envInt("CONNECTOR_STREAM_BATCH_SIZE", 10)),
		ConnectorStreamMaxDeliveries: int64(envInt("CONNECTOR_STREAM_MAX_DELIVERIES", 10)),
		ConnectorStreamDeadLetterKey: getEnvOrDefault("CONNECTOR_STREAM_DEAD_LETTER_KEY", "connector-tasks-dead"),

		OpenBaoAddr:    getEnvOrDefault("OPENBAO_ADDR", ""),
		OpenBaoToken:   getEnvOrDefault("OPENBAO_TOKEN", ""),
		OpenBaoMount:   getEnvOrDefault("OPENBAO_MOUNT", "secret"),
		OpenBaoTimeout: envDuration("OPENBAO_TIMEOUT", 5*time.Second),

		ConnectorAliasFetchTimeout:    envDuration("CONNECTOR_ALIAS_FETCH_TIMEOUT", 10*time.Second),
		ConnectorAliasRefreshInterval: envDuration("CONNECTOR_ALIAS_REFRESH_INTERVAL", 5*time.Minute),

		ConnectorPoolSizeStorage:         envInt("CONNECTOR_POOL_SIZE_STORAGE", 10),
		ConnectorPoolSizeSendEmail:       envInt("CONNECTOR_POOL_SIZE_SEND_EMAIL", 5),
		ConnectorPoolSizeDocumentExtract: envInt("CONNECTOR_POOL_SIZE_DOCUMENT_EXTRACT", 10),
		ConnectorPoolSizeRestCall:        envInt("CONNECTOR_POOL_SIZE_REST_CALL", 20),
		ConnectorPoolSizeSQLQuery:        envInt("CONNECTOR_POOL_SIZE_SQL_QUERY", 15),
		ConnectorPoolSizeChatNotify:      envInt("CONNECTOR_POOL_SIZE_CHAT_NOTIFY", 5),

		ConnectorTimeoutStorage:         envDuration("CONNECTOR_TIMEOUT_STORAGE", 60*time.Second),
		ConnectorTimeoutSendEmail:       envDuration("CONNECTOR_TIMEOUT_SEND_EMAIL", 10*time.Second),
		ConnectorTimeoutDocumentExtract: envDuration("CONNECTOR_TIMEOUT_DOCUMENT_EXTRACT", 60*time.Second),
		ConnectorTimeoutRestCall:        envDuration("CONNECTOR_TIMEOUT_REST_CALL", 30*time.Second),
		ConnectorTimeoutSQLQuery:        envDuration("CONNECTOR_TIMEOUT_SQL_QUERY", 15*time.Second),
		ConnectorTimeoutChatNotify:      envDuration("CONNECTOR_TIMEOUT_CHAT_NOTIFY", 10*time.Second),

		ExecutionServiceInternalAddr: getEnvOrDefault("EXECUTION_SERVICE_INTERNAL_ADDR", ""),
		ConnectorCompletionTimeout:   envDuration("CONNECTOR_COMPLETION_TIMEOUT", 5*time.Second),

		ConnectorDocsBucket:          getEnvOrDefault("CONNECTOR_DOCS_BUCKET", ""),
		ConnectorRetryMaxAttempts:    envInt("CONNECTOR_RETRY_MAX_ATTEMPTS", 5),
		ConnectorRetryInitialBackoff: envDuration("CONNECTOR_RETRY_INITIAL_BACKOFF", 500*time.Millisecond),
		ConnectorRetryMaxBackoff:     envDuration("CONNECTOR_RETRY_MAX_BACKOFF", 10*time.Second),
		ConnectorAuditRetention:      envDuration("CONNECTOR_AUDIT_RETENTION", 90*24*time.Hour),
	}

	if len(parseErrs) > 0 {
		return nil, fmt.Errorf("invalid environment variable(s): %s", strings.Join(parseErrs, "; "))
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

var appEnvs = []string{"local", "dev", "test", "staging", "prod"}

func (c *Config) IsDev() bool {
	return c.AppEnv == "dev" || c.AppEnv == "local"
}

func (c *Config) validate() error {
	if !slices.Contains(appEnvs, c.AppEnv) {
		return fmt.Errorf("APP_ENV must be one of %s", strings.Join(appEnvs, ", "))
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("HTTP_PORT must be in [1, 65535]")
	}
	if c.MetricsPort < 1 || c.MetricsPort > 65535 {
		return fmt.Errorf("METRICS_PORT must be in [1, 65535]")
	}
	if c.WorkerHealthPort < 1 || c.WorkerHealthPort > 65535 {
		return fmt.Errorf("WORKER_HEALTH_PORT must be in [1, 65535]")
	}
	if c.WorkerMetricsPort < 1 || c.WorkerMetricsPort > 65535 {
		return fmt.Errorf("WORKER_METRICS_PORT must be in [1, 65535]")
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
	if c.RLSViolationExportInterval <= 0 {
		return fmt.Errorf("RLS_VIOLATION_EXPORT_INTERVAL must be > 0")
	}
	if c.RLSViolationRetention <= 0 {
		return fmt.Errorf("RLS_VIOLATION_RETENTION must be > 0")
	}
	if c.OutboxRetention <= 0 {
		return fmt.Errorf("OUTBOX_RETENTION must be > 0")
	}
	if c.TenantPurgeEnabled && c.TenantHistoryRetention <= 0 {
		return fmt.Errorf("TENANT_HISTORY_RETENTION must be > 0 when TENANT_PURGE_ENABLED is true")
	}
	if c.ProcessedEventRetention <= 0 {
		return fmt.Errorf("PROCESSED_EVENT_RETENTION must be > 0")
	}
	if c.RetentionSweepInterval <= 0 {
		return fmt.Errorf("RETENTION_SWEEP_INTERVAL must be > 0")
	}
	if c.RetentionSweepBatch <= 0 {
		return fmt.Errorf("RETENTION_SWEEP_BATCH must be > 0")
	}
	if c.OperationalStatsInterval <= 0 {
		return fmt.Errorf("OPERATIONAL_STATS_INTERVAL must be > 0")
	}
	if c.OutboxPollInterval <= 0 {
		return fmt.Errorf("OUTBOX_POLL_INTERVAL must be > 0")
	}
	if c.DefinitionClientTimeout <= 0 {
		return fmt.Errorf("DEFINITION_CLIENT_TIMEOUT must be > 0")
	}
	if c.EligibilityClientTimeout <= 0 {
		return fmt.Errorf("ELIGIBILITY_CLIENT_TIMEOUT must be > 0")
	}
	if c.TokenServiceClientTimeout <= 0 {
		return fmt.Errorf("TOKEN_SERVICE_CLIENT_TIMEOUT must be > 0")
	}
	if c.IAMClientTimeout <= 0 {
		return fmt.Errorf("IAM_CLIENT_TIMEOUT must be > 0")
	}
	if c.QueueTopologyPollInterval <= 0 {
		return fmt.Errorf("QUEUE_TOPOLOGY_POLL_INTERVAL must be > 0")
	}
	return nil
}

func (c *Config) RequireMigrate() error {
	if c.MigrationDSN() == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	return nil
}

func (c *Config) RequireServer() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.DefinitionServiceURL == "" {
		return fmt.Errorf("DEFINITION_SERVICE_URL is required")
	}
	if c.EligibilityBaseURL == "" {
		return fmt.Errorf("ELIGIBILITY_BASE_URL is required")
	}
	if c.AWSUseStub && !c.IsDev() {
		return fmt.Errorf("AWS_USE_STUB must be false outside dev and local")
	}
	if !c.AWSUseStub {
		if c.SNSTopicARN == "" {
			return fmt.Errorf("SNS_TOPIC_ARN is required when AWS_USE_STUB=false")
		}
		if c.GlueRegistryName == "" {
			return fmt.Errorf("GLUE_REGISTRY_NAME is required when AWS_USE_STUB=false")
		}
	}
	if !c.IsDev() {
		if c.InternalAPIToken == "" {
			return fmt.Errorf("INTERNAL_API_TOKEN is required outside dev and local")
		}
		if c.DefinitionServiceToken == "" {
			return fmt.Errorf("DEFINITION_SERVICE_TOKEN is required outside dev and local")
		}
		if c.TokenServiceBaseURL == "" {
			return fmt.Errorf("TOKEN_SERVICE_BASE_URL is required outside dev and local")
		}
		if c.OutboxRelayDatabaseURL == "" {
			return fmt.Errorf("OUTBOX_RELAY_DATABASE_URL is required outside dev and local — a missing relay DSN would silently fall back to the RLS-enforced app role and see zero rows forever")
		}
	}
	return nil
}

func (c *Config) RequireWorker() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.DefinitionServiceURL == "" {
		return fmt.Errorf("DEFINITION_SERVICE_URL is required")
	}
	if !c.IsDev() && c.DefinitionServiceToken == "" {
		return fmt.Errorf("DEFINITION_SERVICE_TOKEN is required outside dev and local")
	}
	return nil
}

func (c *Config) RequireConnectorWorker() error {
	if c.DefinitionServiceURL == "" {
		return fmt.Errorf("DEFINITION_SERVICE_URL is required")
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.ConnectorRetryMaxAttempts < 1 {
		return fmt.Errorf("CONNECTOR_RETRY_MAX_ATTEMPTS must be at least 1")
	}
	if c.ConnectorRetryInitialBackoff <= 0 || c.ConnectorRetryMaxBackoff < c.ConnectorRetryInitialBackoff {
		return fmt.Errorf("CONNECTOR_RETRY_MAX_BACKOFF must not be below CONNECTOR_RETRY_INITIAL_BACKOFF, which must be positive")
	}
	if !c.IsDev() {
		if c.DefinitionServiceToken == "" {
			return fmt.Errorf("DEFINITION_SERVICE_TOKEN is required outside dev and local")
		}
		if c.InternalAPIToken == "" {
			return fmt.Errorf("INTERNAL_API_TOKEN is required outside dev and local")
		}
		if c.OpenBaoToken == "" {
			return fmt.Errorf("OPENBAO_TOKEN is required outside dev and local")
		}
		if c.ConnectorDocsBucket == "" {
			return fmt.Errorf("CONNECTOR_DOCS_BUCKET is required outside dev and local")
		}
		if c.OutboxRelayDatabaseURL == "" {
			return fmt.Errorf("OUTBOX_RELAY_DATABASE_URL is required outside dev and local: the connector audit prune needs the relay role, which bypasses row-level security")
		}
	}
	return nil
}

func (c *Config) OutboxRelayDSN() string {
	if c.OutboxRelayDatabaseURL != "" {
		return c.OutboxRelayDatabaseURL
	}
	return c.DatabaseURL
}

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

func getEnvIntOrDefault(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return i, nil
}

func getEnvBoolOrDefault(key string, fallback bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return b, nil
}

func getEnvFloat64OrDefault(key string, fallback float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return f, nil
}

func getEnvDurationOrDefault(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return d, nil
}

func VersionRequested() bool {
	return len(os.Args) > 1 && os.Args[1] == "version"
}
