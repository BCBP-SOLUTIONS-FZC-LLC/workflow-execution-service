package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_GlueSchemaMaxStaleness_DefaultsToOneHour(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()

	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.GlueSchemaMaxStaleness != time.Hour {
		t.Errorf("GlueSchemaMaxStaleness = %v, want 1h", cfg.GlueSchemaMaxStaleness)
	}
}

func TestLoad_GlueSchemaMaxStaleness_ReadsTheVariableAndAllowsZero(t *testing.T) {
	for value, want := range map[string]time.Duration{"15m": 15 * time.Minute, "0s": 0} {
		t.Run(value, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("GLUE_SCHEMA_MAX_STALENESS", value)

			cfg, err := Load()

			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.GlueSchemaMaxStaleness != want {
				t.Errorf("GlueSchemaMaxStaleness = %v, want %v", cfg.GlueSchemaMaxStaleness, want)
			}
		})
	}
}

func TestLoad_GlueSchemaMaxStaleness_RefusesNegative(t *testing.T) {
	const key = "GLUE_SCHEMA_MAX_STALENESS"

	err := cfgfixLoadWith(t, key, "-1m")

	if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), ">= 0") {
		t.Fatalf("Load() error = %v, want one naming %s and the rule >= 0", err, key)
	}
}
