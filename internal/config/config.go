package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv       string
	BuildVersion string

	HTTPPort         int
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

	// DefinitionServiceURL is the Definition Service's internal HTTP base URL
	// (LLD §5.3), which cmd/server, cmd/worker and cmd/connector-worker all
	// call. DefinitionServiceToken is that service's INTERNAL_API_TOKEN.
	DefinitionServiceURL    string
	DefinitionServiceToken  string
	DefinitionClientTimeout time.Duration

	// EligibilityBaseURL is required for cmd/server (InstanceService/
	// DelegationReconciler's Eligibility field is not nil-safe) but not
	// enforced here: cmd/server fails fast inline instead.
	EligibilityBaseURL       string
	EligibilityClientTimeout time.Duration

	// IAMBaseURL is optional: IAMClient is a permanent stub today
	// (ErrIAMContractNotConfirmed) and TaskService.IAM nil-guards its own
	// use, so an empty value is harmless either way.
	IAMBaseURL       string
	IAMClientTimeout time.Duration

	// TokenServiceBaseURL is the identity subsystem's Token Service, read for
	// each tenant's automation principal. Required by cmd/server outside dev.
	TokenServiceBaseURL       string
	TokenServiceClientTimeout time.Duration

	// QueueTopologyPollInterval is how often cmd/worker polls
	// active_task_queues for isolated tenant queues to start a Worker
	// against (LLD §3.2 item 2, proposed 60s).
	QueueTopologyPollInterval time.Duration

	// InternalAPIToken authenticates the internal route group
	// (middleware.RequireInternalToken). Empty disables
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
	// how long an entry must go untouched by its consumer before another
	// may reclaim it (LLD workflow_connectors.md §6.5 step 0).
	ConnectorStreamClaimMinIdle time.Duration
	ConnectorStreamBatchSize    int64
	// An entry delivered more often than this is moved to
	// ConnectorStreamDeadLetterKey instead of being dispatched again.
	ConnectorStreamMaxDeliveries int64
	ConnectorStreamDeadLetterKey string

	// OpenBao KV-v2 read side — cmd/connector-worker only. Deliberately NOT
	// validated below (see validate()'s own doc comment on why
	// connector-worker-only fields skip that): cmd/connector-worker checks
	// these itself at startup.
	OpenBaoAddr    string
	OpenBaoToken   string
	OpenBaoMount   string
	OpenBaoTimeout time.Duration

	// cmd/connector-worker fetches the alias registry from the Definition
	// Service's GET /api/v1/internal/connector-aliases at DefinitionServiceURL
	// (LLD Decision #21).
	ConnectorAliasFetchTimeout    time.Duration
	ConnectorAliasRefreshInterval time.Duration

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
	// POST /api/v1/internal/connector-tasks/:id/{complete,fail} endpoints.
	// Reuses the existing InternalAPIToken field as the shared secret those
	// endpoints already check — connector-worker-only, not validated here.
	ExecutionServiceInternalAddr string
	ConnectorCompletionTimeout   time.Duration
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

		OTELServiceName:        getEnvOrDefault("OTEL_SERVICE_NAME", "execution-service"),
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

		OutboxPollInterval:     envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:        envInt("OUTBOX_BATCH_SIZE", 50),
		OutboxRelayDatabaseURL: getEnvOrDefault("OUTBOX_RELAY_DATABASE_URL", ""),

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
	}

	if len(parseErrs) > 0 {
		return nil, fmt.Errorf("invalid environment variable(s): %s", strings.Join(parseErrs, "; "))
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate checks value ranges only. What a binary requires to be set is
// checked by its own Require method, so the environment of one binary does
// not have to carry another's settings.
func (c *Config) validate() error {
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

// RequireMigrate checks what the migrate subcommand needs: a database.
func (c *Config) RequireMigrate() error {
	if c.MigrationDSN() == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	return nil
}

// RequireServer checks what cmd/server needs.
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
	if !c.AWSUseStub {
		if c.SNSTopicARN == "" {
			return fmt.Errorf("SNS_TOPIC_ARN is required when AWS_USE_STUB=false")
		}
		if c.GlueRegistryName == "" {
			return fmt.Errorf("GLUE_REGISTRY_NAME is required when AWS_USE_STUB=false")
		}
	}
	if c.AppEnv != "dev" {
		if c.InternalAPIToken == "" {
			return fmt.Errorf("INTERNAL_API_TOKEN is required when APP_ENV != dev")
		}
		if c.DefinitionServiceToken == "" {
			return fmt.Errorf("DEFINITION_SERVICE_TOKEN is required when APP_ENV != dev")
		}
		if c.TokenServiceBaseURL == "" {
			return fmt.Errorf("TOKEN_SERVICE_BASE_URL is required when APP_ENV != dev")
		}
		if c.OutboxRelayDatabaseURL == "" {
			return fmt.Errorf("OUTBOX_RELAY_DATABASE_URL is required when APP_ENV != dev — a missing relay DSN would silently fall back to the RLS-enforced app role and see zero rows forever")
		}
	}
	return nil
}

// RequireWorker checks what cmd/worker needs. Its Activities only enqueue
// events, so it needs neither the relay DSN nor the SNS and Glue settings,
// and it serves no internal API, so it needs no INTERNAL_API_TOKEN.
func (c *Config) RequireWorker() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.DefinitionServiceURL == "" {
		return fmt.Errorf("DEFINITION_SERVICE_URL is required")
	}
	if c.AppEnv != "dev" && c.DefinitionServiceToken == "" {
		return fmt.Errorf("DEFINITION_SERVICE_TOKEN is required when APP_ENV != dev")
	}
	return nil
}

// RequireConnectorWorker checks what cmd/connector-worker needs that is not
// specific to its stream: it has no database and publishes no events.
func (c *Config) RequireConnectorWorker() error {
	if c.DefinitionServiceURL == "" {
		return fmt.Errorf("DEFINITION_SERVICE_URL is required")
	}
	if c.AppEnv != "dev" {
		if c.DefinitionServiceToken == "" {
			return fmt.Errorf("DEFINITION_SERVICE_TOKEN is required when APP_ENV != dev")
		}
		if c.InternalAPIToken == "" {
			return fmt.Errorf("INTERNAL_API_TOKEN is required when APP_ENV != dev")
		}
	}
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

// getEnvIntOrDefault, getEnvBoolOrDefault, getEnvFloat64OrDefault, and
// getEnvDurationOrDefault return a non-nil error alongside the fallback when
// the variable is set but fails to parse — a typo'd value must fail config
// loading, not silently boot with a default nobody asked for. Load's own
// envInt/envBool/envFloat64/envDuration closures collect these.
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

// VersionRequested reports whether the process was invoked as
// `<binary> version`. Callers check it before Load, so the subcommand works
// on an image carrying no environment at all — which is what makes it
// usable as CI's "this image's binary starts" smoke check. Every other
// entrypoint legitimately needs DATABASE_URL and friends, so none of them
// can serve that purpose.
func VersionRequested() bool {
	return len(os.Args) > 1 && os.Args[1] == "version"
}
