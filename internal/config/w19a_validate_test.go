package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const w19aAppEnvMessage = "APP_ENV must be one of local, dev, test, staging, prod"

func TestLoad_AppEnvOutsideTheKnownSet_IsRefused(t *testing.T) {
	var cases []w19aLoadCase
	for _, value := range []string{"production", "prod ", "DEV", "qa"} {
		cases = append(cases, w19aLoadCase{
			name:    fmt.Sprintf("%q", value),
			env:     map[string]string{"APP_ENV": value},
			wantErr: w19aAppEnvMessage,
		})
	}
	for _, value := range w19aEnvironments {
		cases = append(cases, w19aLoadCase{
			name: fmt.Sprintf("%q is accepted", value),
			env:  map[string]string{"APP_ENV": value},
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, value, cfg.AppEnv)
			},
		})
	}
	w19aRunLoadCases(t, cases)
}

func TestLoad_AppEnvUnsetOrEmpty_IsRefused(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "unset", env: map[string]string{"APP_ENV": unsetEnv}, wantErr: w19aAppEnvMessage},
		{name: "empty", env: map[string]string{"APP_ENV": ""}, wantErr: w19aAppEnvMessage},
	})
}

func TestConfigIsDev_TrueOnlyForDevAndLocal(t *testing.T) {
	cases := []struct {
		appEnv string
		want   bool
	}{
		{"dev", true},
		{"local", true},
		{"test", false},
		{"staging", false},
		{"prod", false},
		{"DEV", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.appEnv), func(t *testing.T) {
			cfg := &Config{AppEnv: tc.appEnv}

			assert.Equal(t, tc.want, cfg.IsDev())
		})
	}
}

func TestLoad_PoolSizeBounds_AreEnforced(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "max connections zero", env: map[string]string{"PG_MAX_CONNS": "0"}, wantErr: "PG_MAX_CONNS must be > 0"},
		{name: "max connections negative", env: map[string]string{"PG_MAX_CONNS": "-1"}, wantErr: "PG_MAX_CONNS must be > 0"},
		{name: "min connections negative", env: map[string]string{"PG_MIN_CONNS": "-1"}, wantErr: "PG_MIN_CONNS must be >= 0"},
		{
			name: "min connections zero is accepted",
			env:  map[string]string{"PG_MIN_CONNS": "0"},
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, int32(0), cfg.PGMinConns)
				assert.Equal(t, int32(10), cfg.PGMaxConns)
			},
		},
	})
}

func TestLoad_MinConnectionsAboveMax_IsRefusedAndEqualIsAccepted(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{
			name:    "max 1 with the default min of 2",
			env:     map[string]string{"PG_MAX_CONNS": "1"},
			wantErr: "PG_MIN_CONNS must be <= PG_MAX_CONNS",
		},
		{
			name:    "min 20 over max 10",
			env:     map[string]string{"PG_MIN_CONNS": "20", "PG_MAX_CONNS": "10"},
			wantErr: "PG_MIN_CONNS must be <= PG_MAX_CONNS",
		},
		{
			name: "min equal to max",
			env:  map[string]string{"PG_MIN_CONNS": "5", "PG_MAX_CONNS": "5"},
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, int32(5), cfg.PGMinConns)
				assert.Equal(t, int32(5), cfg.PGMaxConns)
			},
		},
	})
}

func TestLoad_SamplerRatio_MustBeInTheUnitInterval(t *testing.T) {
	const message = "OTEL_TRACES_SAMPLER_RATIO must be in [0.0, 1.0]"
	accepted := func(want float64) func(*testing.T, *Config) {
		return func(t *testing.T, cfg *Config) { assert.Equal(t, want, cfg.OTELTracesSamplerRatio) }
	}
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "below zero", env: map[string]string{"OTEL_TRACES_SAMPLER_RATIO": "-0.1"}, wantErr: message},
		{name: "above one", env: map[string]string{"OTEL_TRACES_SAMPLER_RATIO": "1.1"}, wantErr: message},
		{name: "zero", env: map[string]string{"OTEL_TRACES_SAMPLER_RATIO": "0"}, check: accepted(0)},
		{name: "one half", env: map[string]string{"OTEL_TRACES_SAMPLER_RATIO": "0.5"}, check: accepted(0.5)},
		{name: "one", env: map[string]string{"OTEL_TRACES_SAMPLER_RATIO": "1"}, check: accepted(1)},
		{
			name:    "not a number is refused at parse",
			env:     map[string]string{"OTEL_TRACES_SAMPLER_RATIO": "abc"},
			wantErr: `invalid environment variable(s): OTEL_TRACES_SAMPLER_RATIO="abc": strconv.ParseFloat: parsing "abc": invalid syntax`,
		},
	})
}

func TestLoad_OutboxBatchAndPollInterval_MustBePositive(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "batch size zero", env: map[string]string{"OUTBOX_BATCH_SIZE": "0"}, wantErr: "OUTBOX_BATCH_SIZE must be > 0"},
		{name: "batch size negative", env: map[string]string{"OUTBOX_BATCH_SIZE": "-1"}, wantErr: "OUTBOX_BATCH_SIZE must be > 0"},
		{name: "poll interval zero", env: map[string]string{"OUTBOX_POLL_INTERVAL": "0"}, wantErr: "OUTBOX_POLL_INTERVAL must be > 0"},
		{name: "poll interval negative", env: map[string]string{"OUTBOX_POLL_INTERVAL": "-1s"}, wantErr: "OUTBOX_POLL_INTERVAL must be > 0"},
	})
}

func TestLoad_ClientTimeouts_MustBePositive(t *testing.T) {
	var cases []w19aLoadCase
	for _, key := range []string{"DEFINITION_CLIENT_TIMEOUT", "ELIGIBILITY_CLIENT_TIMEOUT", "TOKEN_SERVICE_CLIENT_TIMEOUT", "ENTITLEMENTS_CLIENT_TIMEOUT"} {
		for _, value := range []string{"0", "-1s"} {
			cases = append(cases, w19aLoadCase{
				name:    fmt.Sprintf("%s=%s", key, value),
				env:     map[string]string{key: value},
				wantErr: key + " must be > 0",
			})
		}
	}
	w19aRunLoadCases(t, cases)
}

func TestLoad_EntitlementsCacheTTL_MustBePositive(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "zero", env: map[string]string{"ENTITLEMENTS_CACHE_TTL": "0"}, wantErr: "ENTITLEMENTS_CACHE_TTL must be > 0"},
		{name: "negative", env: map[string]string{"ENTITLEMENTS_CACHE_TTL": "-1s"}, wantErr: "ENTITLEMENTS_CACHE_TTL must be > 0"},
	})
}
