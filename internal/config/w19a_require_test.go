package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRequireServer_EmptyAddresses_AreNamedOneAtATimeInEveryEnvironment(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"all three empty names the database first", func(c *Config) {
			c.DatabaseURL, c.DefinitionServiceURL, c.EligibilityBaseURL = "", "", ""
		}, "DATABASE_URL is required"},
		{"database set, definition and eligibility empty names the definition service", func(c *Config) {
			c.DefinitionServiceURL, c.EligibilityBaseURL = "", ""
		}, "DEFINITION_SERVICE_URL is required"},
		{"database and definition set, eligibility empty names the eligibility service", func(c *Config) {
			c.EligibilityBaseURL = ""
		}, "ELIGIBILITY_BASE_URL is required"},
		{"database and eligibility empty names the database", func(c *Config) {
			c.DatabaseURL, c.EligibilityBaseURL = "", ""
		}, "DATABASE_URL is required"},
	}
	for _, appEnv := range w19aEnvironments {
		for _, tc := range cases {
			t.Run(appEnv+" "+tc.name, func(t *testing.T) {
				cfg := w19aLaunchConfig(appEnv)
				tc.mutate(cfg)

				assert.EqualError(t, cfg.RequireServer(), tc.wantErr)
			})
		}
	}
}

func TestRequireWorker_NamesTheDatabaseBeforeTheDefinitionService(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"both empty names the database", func(c *Config) { c.DatabaseURL, c.DefinitionServiceURL = "", "" }, "DATABASE_URL is required"},
		{"database empty alone", func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL is required"},
		{"definition service empty alone", func(c *Config) { c.DefinitionServiceURL = "" }, "DEFINITION_SERVICE_URL is required"},
		{"both set", func(c *Config) {}, ""},
	}
	for _, appEnv := range w19aEnvironments {
		for _, tc := range cases {
			t.Run(appEnv+" "+tc.name, func(t *testing.T) {
				cfg := w19aLaunchConfig(appEnv)
				tc.mutate(cfg)

				err := cfg.RequireWorker()

				if tc.wantErr == "" {
					assert.NoError(t, err)
					return
				}
				assert.EqualError(t, err, tc.wantErr)
			})
		}
	}
}

func TestRequireConnectorWorker_NamesTheDefinitionServiceBeforeTheDatabase(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"both empty names the definition service", func(c *Config) { c.DatabaseURL, c.DefinitionServiceURL = "", "" }, "DEFINITION_SERVICE_URL is required"},
		{"database empty alone", func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL is required"},
		{"definition service empty alone", func(c *Config) { c.DefinitionServiceURL = "" }, "DEFINITION_SERVICE_URL is required"},
	}
	for _, appEnv := range w19aEnvironments {
		for _, tc := range cases {
			t.Run(appEnv+" "+tc.name, func(t *testing.T) {
				cfg := w19aLaunchConfig(appEnv)
				tc.mutate(cfg)

				assert.EqualError(t, cfg.RequireConnectorWorker(), tc.wantErr)
			})
		}
	}
}

func TestRequireConnectorWorker_RetryBackoffBounds(t *testing.T) {
	const message = "CONNECTOR_RETRY_MAX_BACKOFF must not be below CONNECTOR_RETRY_INITIAL_BACKOFF, which must be positive"
	cases := []struct {
		name    string
		initial time.Duration
		max     time.Duration
		wantErr string
	}{
		{"initial zero", 0, 10 * time.Second, message},
		{"initial negative", -time.Second, 10 * time.Second, message},
		{"initial and max zero", 0, 0, message},
		{"max below initial", 10 * time.Second, time.Second, message},
		{"max equal to initial", 2 * time.Second, 2 * time.Second, ""},
		{"max above initial", time.Second, 2 * time.Second, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := w19aLaunchConfig("dev")
			cfg.ConnectorRetryInitialBackoff, cfg.ConnectorRetryMaxBackoff = tc.initial, tc.max

			err := cfg.RequireConnectorWorker()

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestRequireConnectorWorker_SecretAndBucketOutsideDev(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		strict string
	}{
		{
			name: "token empty",
			mutate: func(c *Config) {
				c.OpenBaoToken, c.ConnectorDocsBucket, c.OutboxRelayDatabaseURL = "", "docs", "postgres://relay"
			},
			strict: "OPENBAO_TOKEN is required outside dev and local",
		},
		{
			name: "bucket empty",
			mutate: func(c *Config) {
				c.OpenBaoToken, c.ConnectorDocsBucket, c.OutboxRelayDatabaseURL = "tok", "", "postgres://relay"
			},
			strict: "CONNECTOR_DOCS_BUCKET is required outside dev and local",
		},
		{
			name: "token and bucket empty names the token first",
			mutate: func(c *Config) {
				c.OpenBaoToken, c.ConnectorDocsBucket, c.OutboxRelayDatabaseURL = "", "", "postgres://relay"
			},
			strict: "OPENBAO_TOKEN is required outside dev and local",
		},
		{
			name:   "token, bucket and relay empty",
			mutate: func(c *Config) { c.OpenBaoToken, c.ConnectorDocsBucket, c.OutboxRelayDatabaseURL = "", "", "" },
			strict: "OPENBAO_TOKEN is required outside dev and local",
		},
	}
	for _, appEnv := range w19aEnvironments {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s %s", appEnv, tc.name), func(t *testing.T) {
				cfg := w19aLaunchConfig(appEnv)
				tc.mutate(cfg)

				err := cfg.RequireConnectorWorker()

				if appEnv == "dev" || appEnv == "local" {
					assert.NoError(t, err, "dev and local start without a token, a bucket or a relay database")
					return
				}
				assert.EqualError(t, err, tc.strict)
			})
		}
	}
}
