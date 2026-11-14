package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func cfgfixLoadWith(t *testing.T, key, value string) error {
	t.Helper()
	setValidEnv(t)
	t.Setenv(key, value)
	_, err := Load()
	return err
}

func TestLoad_RefusesZeroOrNegativeTimeoutsIntervalsRetentionAndPoolSizes(t *testing.T) {
	durations := []string{
		"CONNECTOR_ALIAS_REFRESH_INTERVAL",
		"CONNECTOR_ALIAS_FETCH_TIMEOUT",
		"CONNECTOR_COMPLETION_TIMEOUT",
		"CONNECTOR_AUDIT_RETENTION",
		"CONNECTOR_TIMEOUT_STORAGE",
		"CONNECTOR_TIMEOUT_SEND_EMAIL",
		"CONNECTOR_TIMEOUT_DOCUMENT_EXTRACT",
		"CONNECTOR_TIMEOUT_REST_CALL",
		"CONNECTOR_TIMEOUT_SQL_QUERY",
		"CONNECTOR_TIMEOUT_CHAT_NOTIFY",
		"OPENBAO_TIMEOUT",
		"VALKEY_DIAL_TIMEOUT",
		"VALKEY_READ_TIMEOUT",
		"VALKEY_WRITE_TIMEOUT",
		"IDEMPOTENCY_TTL",
	}
	for _, key := range durations {
		for _, value := range []string{"0s", "-1s"} {
			t.Run(key+"="+value, func(t *testing.T) {
				err := cfgfixLoadWith(t, key, value)
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("Load() error = %v, want one naming %s", err, key)
				}
			})
		}
	}
	sizes := []string{
		"CONNECTOR_POOL_SIZE_STORAGE",
		"CONNECTOR_POOL_SIZE_SEND_EMAIL",
		"CONNECTOR_POOL_SIZE_DOCUMENT_EXTRACT",
		"CONNECTOR_POOL_SIZE_REST_CALL",
		"CONNECTOR_POOL_SIZE_SQL_QUERY",
		"CONNECTOR_POOL_SIZE_CHAT_NOTIFY",
	}
	for _, key := range sizes {
		for _, value := range []string{"0", "-3"} {
			t.Run(key+"="+value, func(t *testing.T) {
				err := cfgfixLoadWith(t, key, value)
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("Load() error = %v, want one naming %s", err, key)
				}
			})
		}
	}
}

func TestLoad_AcceptsPositiveConnectorSettings(t *testing.T) {
	setValidEnv(t)
	t.Setenv("CONNECTOR_AUDIT_RETENTION", "1h")
	t.Setenv("CONNECTOR_TIMEOUT_REST_CALL", "1ms")
	t.Setenv("CONNECTOR_POOL_SIZE_REST_CALL", "1")
	t.Setenv("VALKEY_DIAL_TIMEOUT", "1ms")

	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, want nil: any positive value is accepted", err)
	}
}

func TestLoad_SeparatorOnlyStartRefusedStatusesTakesTheDefault(t *testing.T) {
	want := []string{"suspended", "trial_expired", "cancelled"}
	for _, raw := range []string{",,", " ", ", ,", " , "} {
		t.Run("value "+raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("START_REFUSED_STATUSES", raw)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if !slices.Equal(cfg.StartRefusedStatuses, want) {
				t.Fatalf("StartRefusedStatuses = %v, want the default %v: a stray separator must not switch the guard off", cfg.StartRefusedStatuses, want)
			}
		})
	}
}

func TestLoad_RefusesSamplerRatioThatIsNotANumberOrInfinite(t *testing.T) {
	for _, value := range []string{"NaN", "nan", "Inf", "+Inf", "-Inf"} {
		t.Run(value, func(t *testing.T) {
			err := cfgfixLoadWith(t, "OTEL_TRACES_SAMPLER_RATIO", value)
			if err == nil || !strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER_RATIO") {
				t.Fatalf("Load() error = %v, want one naming OTEL_TRACES_SAMPLER_RATIO", err)
			}
		})
	}
}

func cfgfixWorkDirWithEnvFile(t *testing.T, content string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
}

func TestLoad_MalformedDotEnvFileFailsTheLoadNamingTheFile(t *testing.T) {
	setValidEnv(t)
	cfgfixWorkDirWithEnvFile(t, "HTTP_PORT=\"unterminated\n")

	_, err := Load()

	if err == nil || !strings.Contains(err.Error(), ".env") {
		t.Fatalf("Load() error = %v, want one naming the .env file", err)
	}
}

func TestLoad_WellFormedDotEnvFileFillsUnsetVariables(t *testing.T) {
	setValidEnv(t)
	t.Setenv("WORKFLOW_CACHE_SIZE", "")
	if err := os.Unsetenv("WORKFLOW_CACHE_SIZE"); err != nil {
		t.Fatal(err)
	}
	cfgfixWorkDirWithEnvFile(t, "WORKFLOW_CACHE_SIZE=77\n")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WorkflowCacheSize != 77 {
		t.Fatalf("WorkflowCacheSize = %d, want 77 from the .env file", cfg.WorkflowCacheSize)
	}
}

func TestLoad_MissingDotEnvFileIsNotAnError(t *testing.T) {
	setValidEnv(t)
	t.Chdir(t.TempDir())

	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, want nil when no .env file exists", err)
	}
}

func TestLoad_RefusesTwoListenPortsThatAreEqual(t *testing.T) {
	ports := []string{"HTTP_PORT", "METRICS_PORT", "WORKER_HEALTH_PORT", "WORKER_METRICS_PORT"}
	for i, a := range ports {
		for _, b := range ports[i+1:] {
			t.Run(a+" equals "+b, func(t *testing.T) {
				setValidEnv(t)
				t.Setenv(a, "7777")
				t.Setenv(b, "7777")

				_, err := Load()

				if err == nil || !strings.Contains(err.Error(), a) || !strings.Contains(err.Error(), b) {
					t.Fatalf("Load() error = %v, want one naming both %s and %s", err, a, b)
				}
			})
		}
	}
}

func TestLoad_AcceptsDistinctListenPorts(t *testing.T) {
	setValidEnv(t)

	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, want nil for the four default ports", err)
	}
}

func cfgfixServerConfig(tokenServiceURL string) *Config {
	return &Config{
		AppEnv:               "dev",
		DatabaseURL:          "postgres://x",
		DefinitionServiceURL: "http://definition:8080",
		EligibilityBaseURL:   "http://eligibility",
		TokenServiceBaseURL:  tokenServiceURL,
		AWSUseStub:           true,
	}
}

func TestRequireServer_IdentityServiceAddressIsRequiredInEveryEnvironment(t *testing.T) {
	for _, env := range []string{"dev", "local"} {
		t.Run(env+" with no address", func(t *testing.T) {
			cfg := cfgfixServerConfig("")
			cfg.AppEnv = env

			err := cfg.RequireServer()

			if err == nil || !strings.Contains(err.Error(), "TOKEN_SERVICE_BASE_URL") {
				t.Fatalf("RequireServer() error = %v, want one naming TOKEN_SERVICE_BASE_URL", err)
			}
		})
	}
}

func TestRequireServer_RefusesAMalformedIdentityServiceAddress(t *testing.T) {
	for _, bad := range []string{"token-service", "ftp://token-service", "http://", "://x", "http://exa mple.com"} {
		t.Run(bad, func(t *testing.T) {
			err := cfgfixServerConfig(bad).RequireServer()

			if err == nil || !strings.Contains(err.Error(), "TOKEN_SERVICE_BASE_URL") {
				t.Fatalf("RequireServer() error = %v, want one naming TOKEN_SERVICE_BASE_URL", err)
			}
		})
	}
}

func TestRequireServer_AcceptsAValidIdentityServiceAddress(t *testing.T) {
	for _, good := range []string{"http://localhost:8084", "https://token-service.internal"} {
		if err := cfgfixServerConfig(good).RequireServer(); err != nil {
			t.Fatalf("RequireServer(%q) error = %v, want nil", good, err)
		}
	}
}

// A malformed .env file stops the load without quoting the file: the parser's message carries the rest of the
// file, which can hold secrets and connection strings.
func TestLoad_MalformedDotEnvFileErrorWithholdsTheFileContent(t *testing.T) {
	setValidEnv(t)
	cfgfixWorkDirWithEnvFile(t, "$$$\nOPENBAO_TOKEN=cfgfix-secret-marker\nDATABASE_URL=postgres://u:cfgfix-password@db/x\n")

	_, err := Load()

	if err == nil || !strings.Contains(err.Error(), ".env") {
		t.Fatalf("Load() error = %v, want one naming the .env file", err)
	}
	for _, secret := range []string{"cfgfix-secret-marker", "cfgfix-password"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("Load() error quotes the file content: %v", err)
		}
	}
}
