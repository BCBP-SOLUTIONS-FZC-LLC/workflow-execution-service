package config

import (
	"fmt"
	"time"
)

type namedDuration struct {
	name  string
	value time.Duration
}

type namedInt struct {
	name  string
	value int
}

func (c *Config) validatePositiveSettings() error {
	durations := []namedDuration{
		{"VALKEY_DIAL_TIMEOUT", c.ValkeyDialTimeout},
		{"VALKEY_READ_TIMEOUT", c.ValkeyReadTimeout},
		{"VALKEY_WRITE_TIMEOUT", c.ValkeyWriteTimeout},
		{"IDEMPOTENCY_TTL", c.IdempotencyTTL},
		{"OPENBAO_TIMEOUT", c.OpenBaoTimeout},
		{"CONNECTOR_ALIAS_FETCH_TIMEOUT", c.ConnectorAliasFetchTimeout},
		{"CONNECTOR_ALIAS_REFRESH_INTERVAL", c.ConnectorAliasRefreshInterval},
		{"CONNECTOR_COMPLETION_TIMEOUT", c.ConnectorCompletionTimeout},
		{"CONNECTOR_AUDIT_RETENTION", c.ConnectorAuditRetention},
		{"CONNECTOR_STREAM_DEAD_LETTER_RETENTION", c.ConnectorStreamDeadLetterRetention},
		{"CONNECTOR_TIMEOUT_STORAGE", c.ConnectorTimeoutStorage},
		{"CONNECTOR_TIMEOUT_SEND_EMAIL", c.ConnectorTimeoutSendEmail},
		{"CONNECTOR_TIMEOUT_DOCUMENT_EXTRACT", c.ConnectorTimeoutDocumentExtract},
		{"CONNECTOR_TIMEOUT_REST_CALL", c.ConnectorTimeoutRestCall},
		{"CONNECTOR_TIMEOUT_SQL_QUERY", c.ConnectorTimeoutSQLQuery},
		{"CONNECTOR_TIMEOUT_CHAT_NOTIFY", c.ConnectorTimeoutChatNotify},
	}
	for _, d := range durations {
		if d.value <= 0 {
			return fmt.Errorf("%s must be > 0", d.name)
		}
	}
	sizes := []namedInt{
		{"CONNECTOR_POOL_SIZE_STORAGE", c.ConnectorPoolSizeStorage},
		{"CONNECTOR_POOL_SIZE_SEND_EMAIL", c.ConnectorPoolSizeSendEmail},
		{"CONNECTOR_POOL_SIZE_DOCUMENT_EXTRACT", c.ConnectorPoolSizeDocumentExtract},
		{"CONNECTOR_POOL_SIZE_REST_CALL", c.ConnectorPoolSizeRestCall},
		{"CONNECTOR_POOL_SIZE_SQL_QUERY", c.ConnectorPoolSizeSQLQuery},
		{"CONNECTOR_POOL_SIZE_CHAT_NOTIFY", c.ConnectorPoolSizeChatNotify},
	}
	for _, s := range sizes {
		if s.value <= 0 {
			return fmt.Errorf("%s must be > 0", s.name)
		}
	}
	return nil
}

func (c *Config) validateDistinctPorts() error {
	ports := []namedInt{
		{"HTTP_PORT", c.HTTPPort},
		{"METRICS_PORT", c.MetricsPort},
		{"WORKER_HEALTH_PORT", c.WorkerHealthPort},
		{"WORKER_METRICS_PORT", c.WorkerMetricsPort},
	}
	for i, a := range ports {
		for _, b := range ports[i+1:] {
			if a.value == b.value {
				return fmt.Errorf("%s and %s must differ: both are %d", a.name, b.name, a.value)
			}
		}
	}
	return nil
}
