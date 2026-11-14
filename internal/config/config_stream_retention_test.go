package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_DeadLetterRetention_DefaultsToThirtyDays(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()

	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ConnectorStreamDeadLetterRetention != 720*time.Hour {
		t.Errorf("ConnectorStreamDeadLetterRetention = %v, want 720h", cfg.ConnectorStreamDeadLetterRetention)
	}
}

func TestLoad_DeadLetterRetention_ReadsTheVariable(t *testing.T) {
	setValidEnv(t)
	t.Setenv("CONNECTOR_STREAM_DEAD_LETTER_RETENTION", "48h")

	cfg, err := Load()

	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ConnectorStreamDeadLetterRetention != 48*time.Hour {
		t.Errorf("ConnectorStreamDeadLetterRetention = %v, want 48h", cfg.ConnectorStreamDeadLetterRetention)
	}
}

func TestLoad_DeadLetterRetention_RefusesZeroAndNegative(t *testing.T) {
	const key = "CONNECTOR_STREAM_DEAD_LETTER_RETENTION"
	for _, value := range []string{"0s", "-1h"} {
		t.Run(value, func(t *testing.T) {
			err := cfgfixLoadWith(t, key, value)
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("Load() error = %v, want one naming %s", err, key)
			}
		})
	}
}
