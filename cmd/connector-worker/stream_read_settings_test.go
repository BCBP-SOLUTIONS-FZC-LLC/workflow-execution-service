package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A negative read block time would make every stream read return at once and spin the read loop, and a batch
// size below one reads nothing useful; both stop the worker at start. A zero block time (wait for entries) stays
// valid.
func TestCheckRequiredConfig_StreamReadSettingsMustBeUsable(t *testing.T) {
	cfg := validConnectorWorkerConfig()
	cfg.ConnectorStreamBlockTimeout = -1
	assert.ErrorContains(t, checkRequiredConfig(cfg), "CONNECTOR_STREAM_BLOCK_TIMEOUT")

	cfg = validConnectorWorkerConfig()
	cfg.ConnectorStreamBatchSize = 0
	assert.ErrorContains(t, checkRequiredConfig(cfg), "CONNECTOR_STREAM_BATCH_SIZE")

	cfg = validConnectorWorkerConfig()
	cfg.ConnectorStreamBlockTimeout = 0
	assert.NoError(t, checkRequiredConfig(cfg))
}
