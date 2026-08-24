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
	MetricsPort      int
	WorkerHealthPort int
	// WorkerMetricsPort isolates cmd/worker's /metrics onto its own listener,
	// separate from WorkerHealthPort's /healthz+/readyz — mirrors cmd/server's
	// HTTPPort/MetricsPort split, so a NetworkPolicy port rule can restrict
	// metrics scraping without also gating liveness/readiness probes.
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
	// MigrationDatabaseURL overrides DatabaseURL for the migrate subcommand.
	// Set to a direct Postgres DSN when DATABASE_URL points to PgBouncer —
	// golang-migrate uses session advisory locks that PgBouncer transaction
	// pooling drops. Falls back to DatabaseURL when unset.
	MigrationDatabaseURL string

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
	OutboxBatchSize    int

	// OutboxRelayDatabaseURL is the outbox relay's own connection string —
	// a distinct role with BYPASSRLS, since the relay must scan every
	// tenant's unpublished rows in one query (db/migrations' own
	// 000005_outbox_rls.up.sql enables RLS on outbox_events). Required
	// outside dev: a silent fallback to DatabaseURL would run the relay
	// under the RLS-enforced app role with no tenant GUC set, and
	// app_tenant_id() returning NULL means it would see zero rows forever,
	// with no error — a silently wedged event pipeline, worse than a
	// startup failure. Local dev's single Postgres role is a superuser
	// (bypasses RLS regardless), which is why the fallback is safe there
	// only. Use OutboxRelayDSN(), not this field directly.
	OutboxRelayDatabaseURL string

	// DefinitionServiceAddr is optional: empty disables the outbound
	// DefinitionClient. Whoever builds the composition root must guard
	// construction on non-empty, mirroring definition_service's own
	// `if cfg.ExecutionServiceAddr != ""` pattern — this is deliberately not
	// enforced in validate() below.
	DefinitionServiceAddr   string
	DefinitionClientTimeout time.Duration

	// EligibilityBaseURL is required for cmd/server (InstanceService/
	// DelegationReconciler's Eligibility field is not nil-safe) but not
	// enforced here — deliberately not in validate(), same convention as
	// DefinitionServiceAddr above; cmd/server fails fast inline instead.
	EligibilityBaseURL       string
	EligibilityClientTimeout time.Duration

	// IAMBaseURL is optional: IAMClient is a permanent stub today
	// (ErrIAMContractNotConfirmed) and TaskService.IAM nil-guards its own
	// use, so an empty value is harmless either way.
	IAMBaseURL       string
	IAMClientTimeout time.Duration

	// QueueTopologyPollInterval is how often cmd/worker polls
	// active_task_queues for isolated tenant queues to start a Worker
	// against (LLD §3.2 item 2, proposed 60s).
	QueueTopologyPollInterval time.Duration

	// InternalAPIToken authenticates internal-only routes/RPCs
	// (middleware.RequireInternalToken, grpc.NewGRPCServer). Empty disables
	// the check — dev mode only; required in prod (see validate()).
	InternalAPIToken string

	// GlueRegistryName/GlueSchemaCacheTTL configure the Glue schema-registry
	// codec — only meaningful when AWSUseStub is false.
	GlueRegistryName   string
	GlueSchemaCacheTTL time.Duration

	// ConnectorStreamKey/Group are shared between cmd/server (which
	// publishes onto the Stream via internal_events.go's workflow.task.created
	// handler) and cmd/connector-worker (which consumes it) — both need
	// sensible defaults, unlike the connector-worker-only fields below.
	ConnectorStreamKey   string
	ConnectorStreamGroup string
	// ConnectorStreamConsumerName is cmd/connector-worker-only; left empty
	// here on purpose — main.go resolves an os.Hostname() fallback when
	// unset, since config.go itself does no such host-environment lookups
	// elsewhere.
	ConnectorStreamConsumerName string
	ConnectorStreamBlockTimeout time.Duration
	// ConnectorStreamClaimMinIdle is XAUTOCLAIM's eligibility threshold —
	// how long an entry must sit unacked before another consumer may
	// reclaim it (LLD workflow_connectors.md §6.5 step 0 / Decision #10).
	ConnectorStreamClaimMinIdle time.Duration
	ConnectorStreamBatchSize    int64

	// OpenBao KV-v2 read side — cmd/connector-worker only. Deliberately NOT
	// validated below (see validate()'s own doc comment on why
	// connector-worker-only fields skip that): cmd/connector-worker checks
	// these itself at startup, mirroring DefinitionServiceAddr's precedent.
	OpenBaoAddr    string
	OpenBaoToken   string
	OpenBaoMount   string
	OpenBaoTimeout time.Duration

	// ConnectorAliasRegistryPath is the static endpointAlias/queryAlias
	// registry file cmd/connector-worker loads at startup via
	// workflow-connectors' own aliasconfig.Load (LLD Decision #12) —
	// connector-worker-only, not validated here.
	ConnectorAliasRegistryPath string

	// Per-connector-type dispatch pool sizing + internal execution timeout
	// (LLD §6.5 step 2 — one bounded pool + one timeout per connector type).
	// A flat field-per-type list, not a nested structure: config.Load() has
	// no existing convention for parsing a map from env, and six types is
	// small enough this doesn't need one. connector-worker-only.
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

	// ExecutionServiceInternalAddr/ConnectorCompletionTimeout: where
	// cmd/connector-worker calls the new
	// POST /internal/connector-tasks/:id/{complete,fail} endpoints.
	// Reuses the existing InternalAPIToken field as the shared secret those
	// endpoints already check — connector-worker-only, not validated here.
	ExecutionServiceInternalAddr string
	ConnectorCompletionTimeout   time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		AppEnv:       getEnvOrDefault("APP_ENV", "dev"),
		BuildVersion: getEnvOrDefault("BUILD_VERSION", "dev"),

		HTTPPort:          getEnvIntOrDefault("HTTP_PORT", 8080),
		GRPCPort:          getEnvIntOrDefault("GRPC_PORT", 9090),
		MetricsPort:       getEnvIntOrDefault("METRICS_PORT", 9091),
		WorkerHealthPort:  getEnvIntOrDefault("WORKER_HEALTH_PORT", 8081),
		WorkerMetricsPort: getEnvIntOrDefault("WORKER_METRICS_PORT", 8082),

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

		ValkeyAddr:         getEnvOrDefault("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword:     getEnvOrDefault("VALKEY_PASSWORD", ""),
		ValkeyDialTimeout:  getEnvDurationOrDefault("VALKEY_DIAL_TIMEOUT", 2*time.Second),
		ValkeyReadTimeout:  getEnvDurationOrDefault("VALKEY_READ_TIMEOUT", 1*time.Second),
		ValkeyWriteTimeout: getEnvDurationOrDefault("VALKEY_WRITE_TIMEOUT", 1*time.Second),
		IdempotencyTTL:     getEnvDurationOrDefault("IDEMPOTENCY_TTL", 24*time.Hour),

		TemporalHostPort:  getEnvOrDefault("TEMPORAL_HOST_PORT", "localhost:7233"),
		TemporalNamespace: getEnvOrDefault("TEMPORAL_NAMESPACE", "default"),
		TemporalTaskQueue: getEnvOrDefault("TEMPORAL_TASK_QUEUE", "wf-queue-default"),

		AWSUseStub:     getEnvBoolOrDefault("AWS_USE_STUB", true),
		AWSRegion:      getEnvOrDefault("AWS_REGION", "us-east-1"),
		SNSTopicARN:    getEnvOrDefault("SNS_TOPIC_ARN", ""),
		AWSEndpointURL: getEnvOrDefault("AWS_ENDPOINT_URL", ""),

		OutboxPollInterval:     getEnvDurationOrDefault("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:        getEnvIntOrDefault("OUTBOX_BATCH_SIZE", 50),
		OutboxRelayDatabaseURL: getEnvOrDefault("OUTBOX_RELAY_DATABASE_URL", ""),

		DefinitionServiceAddr:   getEnvOrDefault("DEFINITION_SERVICE_ADDR", ""),
		DefinitionClientTimeout: getEnvDurationOrDefault("DEFINITION_CLIENT_TIMEOUT", 5*time.Second),

		EligibilityBaseURL:       getEnvOrDefault("ELIGIBILITY_BASE_URL", ""),
		EligibilityClientTimeout: getEnvDurationOrDefault("ELIGIBILITY_CLIENT_TIMEOUT", 5*time.Second),

		IAMBaseURL:       getEnvOrDefault("IAM_BASE_URL", ""),
		IAMClientTimeout: getEnvDurationOrDefault("IAM_CLIENT_TIMEOUT", 5*time.Second),

		QueueTopologyPollInterval: getEnvDurationOrDefault("QUEUE_TOPOLOGY_POLL_INTERVAL", 60*time.Second),

		InternalAPIToken: getEnvOrDefault("INTERNAL_API_TOKEN", ""),

		GlueRegistryName:   getEnvOrDefault("GLUE_REGISTRY_NAME", ""),
		GlueSchemaCacheTTL: getEnvDurationOrDefault("GLUE_SCHEMA_CACHE_TTL", 5*time.Minute),

		ConnectorStreamKey:          getEnvOrDefault("CONNECTOR_STREAM_KEY", "connector-tasks"),
		ConnectorStreamGroup:        getEnvOrDefault("CONNECTOR_STREAM_GROUP", "connector-worker"),
		ConnectorStreamConsumerName: getEnvOrDefault("CONNECTOR_STREAM_CONSUMER_NAME", ""),
		ConnectorStreamBlockTimeout: getEnvDurationOrDefault("CONNECTOR_STREAM_BLOCK_TIMEOUT", 5*time.Second),
		ConnectorStreamClaimMinIdle: getEnvDurationOrDefault("CONNECTOR_STREAM_CLAIM_MIN_IDLE", 30*time.Second),
		ConnectorStreamBatchSize:    int64(getEnvIntOrDefault("CONNECTOR_STREAM_BATCH_SIZE", 10)),

		OpenBaoAddr:    getEnvOrDefault("OPENBAO_ADDR", ""),
		OpenBaoToken:   getEnvOrDefault("OPENBAO_TOKEN", ""),
		OpenBaoMount:   getEnvOrDefault("OPENBAO_MOUNT", "secret"),
		OpenBaoTimeout: getEnvDurationOrDefault("OPENBAO_TIMEOUT", 5*time.Second),

		ConnectorAliasRegistryPath: getEnvOrDefault("CONNECTOR_ALIAS_REGISTRY_PATH", ""),

		ConnectorPoolSizeStorage:         getEnvIntOrDefault("CONNECTOR_POOL_SIZE_STORAGE", 10),
		ConnectorPoolSizeSendEmail:       getEnvIntOrDefault("CONNECTOR_POOL_SIZE_SEND_EMAIL", 5),
		ConnectorPoolSizeDocumentExtract: getEnvIntOrDefault("CONNECTOR_POOL_SIZE_DOCUMENT_EXTRACT", 10),
		ConnectorPoolSizeRestCall:        getEnvIntOrDefault("CONNECTOR_POOL_SIZE_REST_CALL", 20),
		ConnectorPoolSizeSQLQuery:        getEnvIntOrDefault("CONNECTOR_POOL_SIZE_SQL_QUERY", 15),
		ConnectorPoolSizeChatNotify:      getEnvIntOrDefault("CONNECTOR_POOL_SIZE_CHAT_NOTIFY", 5),

		ConnectorTimeoutStorage:         getEnvDurationOrDefault("CONNECTOR_TIMEOUT_STORAGE", 60*time.Second),
		ConnectorTimeoutSendEmail:       getEnvDurationOrDefault("CONNECTOR_TIMEOUT_SEND_EMAIL", 10*time.Second),
		ConnectorTimeoutDocumentExtract: getEnvDurationOrDefault("CONNECTOR_TIMEOUT_DOCUMENT_EXTRACT", 60*time.Second),
		ConnectorTimeoutRestCall:        getEnvDurationOrDefault("CONNECTOR_TIMEOUT_REST_CALL", 30*time.Second),
		ConnectorTimeoutSQLQuery:        getEnvDurationOrDefault("CONNECTOR_TIMEOUT_SQL_QUERY", 15*time.Second),
		ConnectorTimeoutChatNotify:      getEnvDurationOrDefault("CONNECTOR_TIMEOUT_CHAT_NOTIFY", 10*time.Second),

		ExecutionServiceInternalAddr: getEnvOrDefault("EXECUTION_SERVICE_INTERNAL_ADDR", ""),
		ConnectorCompletionTimeout:   getEnvDurationOrDefault("CONNECTOR_COMPLETION_TIMEOUT", 5*time.Second),
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
	if c.OutboxPollInterval <= 0 {
		return fmt.Errorf("OUTBOX_POLL_INTERVAL must be > 0")
	}
	if !c.AWSUseStub {
		if c.SNSTopicARN == "" {
			return fmt.Errorf("SNS_TOPIC_ARN is required when AWS_USE_STUB=false")
		}
		if c.GlueRegistryName == "" {
			return fmt.Errorf("GLUE_REGISTRY_NAME is required when AWS_USE_STUB=false")
		}
	}
	if c.DefinitionClientTimeout <= 0 {
		return fmt.Errorf("DEFINITION_CLIENT_TIMEOUT must be > 0")
	}
	if c.EligibilityClientTimeout <= 0 {
		return fmt.Errorf("ELIGIBILITY_CLIENT_TIMEOUT must be > 0")
	}
	if c.IAMClientTimeout <= 0 {
		return fmt.Errorf("IAM_CLIENT_TIMEOUT must be > 0")
	}
	if c.QueueTopologyPollInterval <= 0 {
		return fmt.Errorf("QUEUE_TOPOLOGY_POLL_INTERVAL must be > 0")
	}
	if c.AppEnv == "prod" && c.InternalAPIToken == "" {
		return fmt.Errorf("INTERNAL_API_TOKEN is required when APP_ENV=prod")
	}
	if c.AppEnv != "dev" && c.OutboxRelayDatabaseURL == "" {
		return fmt.Errorf("OUTBOX_RELAY_DATABASE_URL is required when APP_ENV != dev — a missing relay DSN would silently fall back to the RLS-enforced app role and see zero rows forever")
	}
	// Connector-worker-only fields (OpenBao*, ConnectorAliasRegistryPath,
	// ConnectorPoolSize*/ConnectorTimeout*, ExecutionServiceInternalAddr)
	// are deliberately NOT validated here, mirroring DefinitionServiceAddr's
	// own precedent above: this Config is shared by cmd/server and
	// cmd/worker too, and a hard requirement here would break their boot
	// for fields only cmd/connector-worker actually needs.
	// cmd/connector-worker's own main.go checks its required fields itself.
	return nil
}

// OutboxRelayDSN returns the outbox relay's own connection string.
// OutboxRelayDatabaseURL is required outside dev (see validate()); this
// fallback to DatabaseURL only ever applies in dev, where the local
// Postgres role is a superuser and bypasses RLS regardless.
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
