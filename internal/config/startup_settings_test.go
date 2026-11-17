package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_ShutdownDrainDelay_DefaultsToFiveSecondsAndReadsTheVariable(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ShutdownDrainDelay != 5*time.Second {
		t.Errorf("ShutdownDrainDelay = %v, want 5s", cfg.ShutdownDrainDelay)
	}

	t.Setenv("SHUTDOWN_DRAIN_DELAY", "0s")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ShutdownDrainDelay != 0 {
		t.Errorf("ShutdownDrainDelay = %v, want 0 (no drain)", cfg.ShutdownDrainDelay)
	}
}

func TestLoad_ShutdownDrainDelay_RefusesNegativeAndOneThatLeavesNoRoomInTheGracePeriod(t *testing.T) {
	const key = "SHUTDOWN_DRAIN_DELAY"
	for _, value := range []string{"-1s", "15s", "1m"} {
		t.Run(value, func(t *testing.T) {
			err := cfgfixLoadWith(t, key, value)
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("Load() error = %v, want one naming %s", err, key)
			}
		})
	}
}

func TestLoad_ProcessedEventRetention_DefaultOutlivesTheFourteenDayQueueRetention(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()

	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ProcessedEventRetention <= 14*24*time.Hour {
		t.Errorf("ProcessedEventRetention = %v: an event redelivered at the end of the 14-day queue retention would be processed twice", cfg.ProcessedEventRetention)
	}
}

func TestLoad_TemporalAddressAndNamespace_DefaultInDevAndStayEmptyElsewhere(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TemporalHostPort != "localhost:7233" || cfg.TemporalNamespace != "default" {
		t.Errorf("dev defaults = %q %q, want localhost:7233 default", cfg.TemporalHostPort, cfg.TemporalNamespace)
	}

	t.Setenv("APP_ENV", "prod")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TemporalHostPort != "" || cfg.TemporalNamespace != "" {
		t.Errorf("prod without the variables = %q %q, want both empty", cfg.TemporalHostPort, cfg.TemporalNamespace)
	}
}

func deployedWorkerConfig() *Config {
	return &Config{
		AppEnv:               "prod",
		DatabaseURL:          "postgres://worker",
		DefinitionServiceURL: "http://definition",
		TemporalHostPort:     "temporal:7233",
		TemporalNamespace:    "workflow",
	}
}

func TestRequireWorker_OutsideDevNeedsTheTemporalAddressAndNamespace(t *testing.T) {
	cases := map[string]func(*Config){
		"TEMPORAL_HOST_PORT": func(c *Config) { c.TemporalHostPort = "" },
		"TEMPORAL_NAMESPACE": func(c *Config) { c.TemporalNamespace = "" },
	}
	for key, unset := range cases {
		t.Run(key, func(t *testing.T) {
			cfg := deployedWorkerConfig()
			unset(cfg)

			err := cfg.RequireWorker()

			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("RequireWorker() error = %v, want one naming %s", err, key)
			}
		})
	}
}

func TestRequireWorker_AcceptsADeployedConfigAndADevOneWithDefaults(t *testing.T) {
	if err := deployedWorkerConfig().RequireWorker(); err != nil {
		t.Errorf("RequireWorker() with both Temporal settings = %v, want nil", err)
	}
	dev := deployedWorkerConfig()
	dev.AppEnv = "dev"
	dev.TemporalHostPort, dev.TemporalNamespace = "", ""
	if err := dev.RequireWorker(); err != nil {
		t.Errorf("RequireWorker() in dev = %v, want nil", err)
	}
}

func deployedServerConfig() *Config {
	return &Config{
		AppEnv:                 "prod",
		DatabaseURL:            "postgres://api",
		DefinitionServiceURL:   "http://definition",
		EligibilityBaseURL:     "http://eligibility",
		TokenServiceBaseURL:    "http://token",
		SNSTopicARN:            "arn:aws:sns:eu-west-1:1:topic",
		GlueRegistryName:       "registry",
		OutboxRelayDatabaseURL: "postgres://relay",
		TemporalHostPort:       "temporal:7233",
		TemporalNamespace:      "workflow",
	}
}

func TestRequireServer_OutsideDevNeedsTheTemporalAddressAndNamespace(t *testing.T) {
	if err := deployedServerConfig().RequireServer(); err != nil {
		t.Fatalf("RequireServer() with every setting = %v, want nil", err)
	}
	cases := map[string]func(*Config){
		"TEMPORAL_HOST_PORT": func(c *Config) { c.TemporalHostPort = "" },
		"TEMPORAL_NAMESPACE": func(c *Config) { c.TemporalNamespace = "" },
	}
	for key, unset := range cases {
		t.Run(key, func(t *testing.T) {
			cfg := deployedServerConfig()
			unset(cfg)

			err := cfg.RequireServer()

			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("RequireServer() error = %v, want one naming %s", err, key)
			}
		})
	}
}

func TestLoad_TemporalClientCertificate_BothPathsOrNeither(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TemporalTLSCertPath != "" || cfg.TemporalTLSKeyPath != "" {
		t.Errorf("paths without the variables = %q %q, want empty", cfg.TemporalTLSCertPath, cfg.TemporalTLSKeyPath)
	}

	t.Setenv("TEMPORAL_TLS_CERT_PATH", "/etc/temporal-tls/tls.crt")
	t.Setenv("TEMPORAL_TLS_KEY_PATH", "/etc/temporal-tls/tls.key")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TemporalTLSCertPath != "/etc/temporal-tls/tls.crt" || cfg.TemporalTLSKeyPath != "/etc/temporal-tls/tls.key" {
		t.Errorf("paths = %q %q, want the variables' values", cfg.TemporalTLSCertPath, cfg.TemporalTLSKeyPath)
	}

	for _, only := range []string{"TEMPORAL_TLS_CERT_PATH", "TEMPORAL_TLS_KEY_PATH"} {
		t.Run(only, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("TEMPORAL_TLS_CERT_PATH", "")
			t.Setenv("TEMPORAL_TLS_KEY_PATH", "")
			t.Setenv(only, "/etc/temporal-tls/file")
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "TEMPORAL_TLS_CERT_PATH") || !strings.Contains(err.Error(), "TEMPORAL_TLS_KEY_PATH") {
				t.Fatalf("Load() error = %v, want one naming both TEMPORAL_TLS_CERT_PATH and TEMPORAL_TLS_KEY_PATH", err)
			}
		})
	}
}

func TestTraceExportEnabled_FollowsTheResolvedEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		exporter string
		want     bool
	}{
		{"unset", "", "", false},
		{"none", "none", "", false},
		{"off", "OFF", "", false},
		{"disabled", " disabled ", "", false},
		{"collector", "otel-collector:4317", "", true},
		{"collector but exporter none", "otel-collector:4317", "none", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_TRACES_EXPORTER", tc.exporter)

			cfg, err := Load()

			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got := cfg.TraceExportEnabled(); got != tc.want {
				t.Errorf("TraceExportEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}
