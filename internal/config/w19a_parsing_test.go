package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_UnparsableVariables_AreListedTogetherAndValidationDoesNotRun(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{
			name: "two bad variables come back in the order they are read",
			env:  map[string]string{"HTTP_PORT": "abc", "OUTBOX_POLL_INTERVAL": "soon"},
			want: []string{
				`invalid environment variable(s): HTTP_PORT="abc": strconv.Atoi: parsing "abc": invalid syntax; `,
				`OUTBOX_POLL_INTERVAL="soon": time: invalid duration "soon"`,
			},
		},
		{
			name: "a range violation is not reported while a variable cannot be parsed",
			env:  map[string]string{"HTTP_PORT": "abc", "OUTBOX_POLL_INTERVAL": "soon", "METRICS_PORT": "0"},
			want: []string{
				`invalid environment variable(s): HTTP_PORT="abc": strconv.Atoi: parsing "abc": invalid syntax; `,
				`OUTBOX_POLL_INTERVAL="soon": time: invalid duration "soon"`,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w19aEnv(t, tc.env)

			cfg, err := Load()

			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Equal(t, strings.Join(tc.want, ""), err.Error(), "both parse errors, nothing else: validate did not run")
		})
	}
}

func TestLoad_TwoRangeViolations_NameOnlyTheFirstFailingRule(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{
			name:    "both ports out of range names HTTP_PORT only",
			env:     map[string]string{"HTTP_PORT": "0", "METRICS_PORT": "0"},
			wantErr: "HTTP_PORT must be in [1, 65535]",
		},
		{
			name:    "once HTTP_PORT is fixed the second violation is named",
			env:     map[string]string{"HTTP_PORT": "8080", "METRICS_PORT": "0"},
			wantErr: "METRICS_PORT must be in [1, 65535]",
		},
	})
}

func TestLoad_EmptyVariable_IsTreatedAsUnset(t *testing.T) {
	cases := []struct {
		key   string
		check func(*Config) w19aCheck
	}{
		{"HTTP_PORT", func(c *Config) w19aCheck { return w19aCheck{"http port is the default", c.HTTPPort, 8080} }},
		{"VALKEY_ADDR", func(c *Config) w19aCheck {
			return w19aCheck{"valkey address is the default", c.ValkeyAddr, "localhost:6379"}
		}},
		{"OUTBOX_POLL_INTERVAL", func(c *Config) w19aCheck {
			return w19aCheck{"poll interval is the default", c.OutboxPollInterval, 500 * time.Millisecond}
		}},
		{"AWS_USE_STUB", func(c *Config) w19aCheck { return w19aCheck{"stub switch is the default", c.AWSUseStub, true} }},
		{"OTEL_TRACES_SAMPLER_RATIO", func(c *Config) w19aCheck {
			return w19aCheck{"sampler ratio is the default", c.OTELTracesSamplerRatio, 1.0}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			w19aEnv(t, map[string]string{tc.key: ""})
			value, present := os.LookupEnv(tc.key)
			require.True(t, present, "the variable is set, to the empty string")
			require.Empty(t, value)

			cfg, err := Load()

			require.NoError(t, err, "an empty value is not a parse error")
			got := tc.check(cfg)
			assert.Equal(t, got.want, got.got, got.name)
		})
	}
}

func TestGetEnvIntOrDefault_Syntax(t *testing.T) {
	const key = "W19A_INT_VALUE"
	const fallback = 7
	cases := []struct {
		name    string
		value   string
		want    int
		refused bool
	}{
		{"plain digits", "8080", 8080, false},
		{"leading plus sign", "+8080", 8080, false},
		{"negative number", "-3", -3, false},
		{"leading space", " 8080", 0, true},
		{"trailing space", "8080 ", 0, true},
		{"decimal point", "80.5", 0, true},
		{"hexadecimal", "0x1F90", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.value)

			got, err := getEnvIntOrDefault(key, fallback)

			if !tc.refused {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
				return
			}
			require.ErrorIs(t, err, strconv.ErrSyntax)
			assert.EqualError(t, err, fmt.Sprintf("%s=%q: strconv.Atoi: parsing %q: invalid syntax", key, tc.value, tc.value))
			assert.Equal(t, fallback, got, "a refused value returns the fallback")
		})
	}
}

func TestGetEnvBoolOrDefault_Spellings(t *testing.T) {
	const key = "W19A_BOOL_VALUE"
	cases := []struct {
		value   string
		want    bool
		refused bool
	}{
		{"1", true, false}, {"t", true, false}, {"T", true, false}, {"TRUE", true, false}, {"true", true, false}, {"True", true, false},
		{"0", false, false}, {"f", false, false}, {"F", false, false}, {"FALSE", false, false}, {"false", false, false}, {"False", false, false},
		{"yes", false, true}, {"on", false, true}, {"enabled", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(key, tc.value)
			fallback := !tc.want
			if tc.refused {
				fallback = true
			}

			got, err := getEnvBoolOrDefault(key, fallback)

			if !tc.refused {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got, "the value is parsed, not replaced by the opposite fallback")
				return
			}
			require.ErrorIs(t, err, strconv.ErrSyntax)
			assert.EqualError(t, err, fmt.Sprintf("%s=%q: strconv.ParseBool: parsing %q: invalid syntax", key, tc.value, tc.value))
			assert.Equal(t, fallback, got, "a refused value returns the fallback")
		})
	}
}

func TestLoad_BooleanVariableSpelledYes_IsRefusedNamingTheKey(t *testing.T) {
	var cases []w19aLoadCase
	for _, key := range []string{"AWS_USE_STUB", "PG_BOUNCER_MODE", "TENANT_PURGE_ENABLED", "OTEL_EXPORTER_OTLP_INSECURE"} {
		cases = append(cases, w19aLoadCase{
			name:    key,
			env:     map[string]string{key: "yes"},
			wantErr: fmt.Sprintf(`invalid environment variable(s): %s="yes": strconv.ParseBool: parsing "yes": invalid syntax`, key),
		})
	}
	w19aRunLoadCases(t, cases)
}

func TestGetEnvDurationOrDefault_Syntax(t *testing.T) {
	const key = "W19A_DURATION_VALUE"
	const fallback = 30 * time.Second
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		refused string
	}{
		{"milliseconds", "500ms", 500 * time.Millisecond, ""},
		{"fraction of a second", "1.5s", 1500 * time.Millisecond, ""},
		{"hours and minutes", "2h45m", 2*time.Hour + 45*time.Minute, ""},
		{"bare zero", "0", 0, ""},
		{"negative", "-5s", -5 * time.Second, ""},
		{"number without a unit", "30", 0, "missing unit in duration"},
		{"space before the unit", "5 s", 0, "unknown unit"},
		{"days", "1d", 0, "unknown unit"},
		{"weeks", "1w", 0, "unknown unit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.value)

			got, err := getEnvDurationOrDefault(key, fallback)

			if tc.refused == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got, "the value is parsed, not replaced by the 30s fallback")
				return
			}
			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), fmt.Sprintf("%s=%q: ", key, tc.value)), "the message names the key and the value: %v", err)
			assert.Contains(t, err.Error(), tc.refused)
			assert.Equal(t, fallback, got, "a refused value returns the fallback")
		})
	}
}

func TestLoad_ProcessEnvironmentWinsOverTheDotEnvFile(t *testing.T) {
	const dotEnv = "WORKFLOW_CACHE_SIZE=77\nRETENTION_SWEEP_BATCH=33\n"
	cases := []struct {
		name      string
		env       map[string]string
		wantCache int
		wantBatch int
	}{
		{
			name:      "a variable of the process is kept, an unset one is filled from the file",
			env:       map[string]string{"WORKFLOW_CACHE_SIZE": "5"},
			wantCache: 5,
			wantBatch: 33,
		},
		{
			name:      "both are filled from the file when the process sets neither",
			env:       nil,
			wantCache: 77,
			wantBatch: 33,
		},
		{
			name:      "both are kept when the process sets both",
			env:       map[string]string{"WORKFLOW_CACHE_SIZE": "5", "RETENTION_SWEEP_BATCH": "9"},
			wantCache: 5,
			wantBatch: 9,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w19aEnv(t, nil)
			w19aForget(t, "WORKFLOW_CACHE_SIZE", "RETENTION_SWEEP_BATCH")
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			require.NoError(t, os.WriteFile(".env", []byte(dotEnv), 0o600))

			cfg, err := Load()

			require.NoError(t, err)
			assert.Equal(t, tc.wantCache, cfg.WorkflowCacheSize)
			assert.Equal(t, tc.wantBatch, cfg.RetentionSweepBatch)
		})
	}
}
