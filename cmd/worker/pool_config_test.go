package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

func TestWorkerPoolConfig_TakesTheSettingsFromTheConfiguration(t *testing.T) {
	cfg := &config.Config{
		DatabaseURL:            "postgres://app@db/exec",
		PGMaxConns:             40,
		PGMinConns:             4,
		PGBouncerMode:          true,
		PGSlowQueryThresholdMS: 250,
		OTELServiceName:        "execution-worker-test",
	}

	got := workerPoolConfig(cfg, nil)

	assert.Equal(t, "postgres://app@db/exec", got.DSN)
	assert.Equal(t, int32(40), got.MaxConns)
	assert.Equal(t, int32(4), got.MinConns)
	assert.True(t, got.PGBouncerMode)
	assert.Equal(t, 250*time.Millisecond, got.SlowQueryThreshold, "the threshold is configured in milliseconds")
	assert.NotNil(t, got.GUCProvider, "every statement carries the tenant setting")
	assert.NotNil(t, got.Logger)
	assert.NotNil(t, got.Tracer)
}
