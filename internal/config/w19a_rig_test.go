package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// w19aEnv gives the test an environment of its own: a fresh working directory without a .env file, every
// inherited variable removed (not merely emptied), then the given ones set. The test must not be parallel.
func w19aEnv(t *testing.T, env map[string]string) {
	t.Helper()
	t.Chdir(t.TempDir())
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if !found || key == "" {
			continue
		}
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
	if _, given := env["APP_ENV"]; !given {
		t.Setenv("APP_ENV", "dev")
	}
	for key, value := range env {
		if value == unsetEnv {
			require.NoError(t, os.Unsetenv(key))
			continue
		}
		t.Setenv(key, value)
	}
}

const unsetEnv = "\x00unset"

// w19aForget removes variables the test did not receive from the environment and arranges for them to be
// removed again afterwards, so a .env file loaded by the test does not leak into other tests.
func w19aForget(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
}

// w19aLoadCase is one Load call in a clean environment. wantErr is the whole message; an empty wantErr means
// Load must succeed and check, when set, inspects the result.
type w19aLoadCase struct {
	name    string
	env     map[string]string
	wantErr string
	check   func(t *testing.T, cfg *Config)
}

func w19aRunLoadCases(t *testing.T, cases []w19aLoadCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w19aEnv(t, tc.env)

			cfg, err := Load()

			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				assert.Nil(t, cfg, "a refused load returns no configuration")
				return
			}
			require.NoError(t, err)
			if tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}

// w19aCheck is one expected default; got and want must have the same comparable type.
type w19aCheck struct {
	name string
	got  any
	want any
}

func w19aRunChecks(t *testing.T, checks []w19aCheck) {
	t.Helper()
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, c.got)
		})
	}
}

var w19aEnvironments = []string{"local", "dev", "test", "staging", "prod"}

// w19aLaunchConfig is a configuration every Require method accepts in dev; cases clear or change one field.
func w19aLaunchConfig(appEnv string) *Config {
	return &Config{
		AppEnv:                       appEnv,
		DatabaseURL:                  "postgres://x",
		DefinitionServiceURL:         "http://definition:8080",
		EligibilityBaseURL:           "http://eligibility",
		TemporalHostPort:             "temporal:7233",
		TemporalNamespace:            "workflow",
		ConnectorRetryMaxAttempts:    5,
		ConnectorRetryInitialBackoff: 500 * time.Millisecond,
		ConnectorRetryMaxBackoff:     10 * time.Second,
	}
}
