package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

func validConnectorWorkerConfig() *config.Config {
	return &config.Config{
		ConnectorStreamKey:           "connector-tasks",
		OpenBaoAddr:                  "http://openbao:8200",
		DefinitionServiceURL:         "http://definition:8080",
		ExecutionServiceInternalAddr: "http://execution:8080",
		ConnectorStreamClaimMinIdle:  30 * time.Second,
		ConnectorStreamMaxDeliveries: 10,
		ConnectorStreamDeadLetterKey: "connector-tasks-dead",
		ConnectorTimeoutStorage:      60 * time.Second,
		ConnectorTimeoutRestCall:     30 * time.Second,
	}
}

func TestCheckRequiredConfig_AcceptsDefaults(t *testing.T) {
	assert.NoError(t, checkRequiredConfig(validConnectorWorkerConfig()))
}

// A recorded outcome must outlive every redelivery that could still read it.
func TestCheckRequiredConfig_OutcomeMustOutliveEveryRedelivery(t *testing.T) {
	cfg := validConnectorWorkerConfig()
	cfg.ConnectorStreamClaimMinIdle = 90 * time.Second
	cfg.ConnectorStreamMaxDeliveries = 500

	assert.ErrorContains(t, checkRequiredConfig(cfg), "CONNECTOR_STREAM_MAX_DELIVERIES")
}

func TestCheckRequiredConfig_RefusesMissingFields(t *testing.T) {
	cases := map[string]func(*config.Config){
		"CONNECTOR_STREAM_KEY":             func(c *config.Config) { c.ConnectorStreamKey = "" },
		"OPENBAO_ADDR":                     func(c *config.Config) { c.OpenBaoAddr = "" },
		"EXECUTION_SERVICE_INTERNAL_ADDR":  func(c *config.Config) { c.ExecutionServiceInternalAddr = "" },
		"CONNECTOR_STREAM_MAX_DELIVERIES":  func(c *config.Config) { c.ConnectorStreamMaxDeliveries = 0 },
		"CONNECTOR_STREAM_DEAD_LETTER_KEY": func(c *config.Config) { c.ConnectorStreamDeadLetterKey = c.ConnectorStreamKey },
		"CONNECTOR_STREAM_CLAIM_MIN_IDLE":  func(c *config.Config) { c.ConnectorStreamClaimMinIdle = time.Second },
	}
	for want, mutate := range cases {
		cfg := validConnectorWorkerConfig()
		mutate(cfg)
		assert.ErrorContains(t, checkRequiredConfig(cfg), want)
	}
}

// Shutdown waits for the slowest run and its callback, so a run that has
// started is never cut off between its side effect and its report.
func TestShutdownDrain_CoversTheSlowestRunAndItsCallback(t *testing.T) {
	cfg := validConnectorWorkerConfig()
	cfg.ConnectorCompletionTimeout = 5 * time.Second

	assert.Equal(t, 60*time.Second+5*time.Second+drainMargin, shutdownDrain(cfg))
}
