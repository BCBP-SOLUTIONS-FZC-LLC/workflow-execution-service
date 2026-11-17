package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

func TestTemporalDialOptions_CarryTheAddressNamespaceAndTracingForAnEnabledExport(t *testing.T) {
	cfg := &config.Config{TemporalHostPort: "temporal:7233", TemporalNamespace: "workflow", OTELExporterEndpoint: "collector:4317"}

	opts, err := temporalDialOptions(cfg)

	require.NoError(t, err)
	assert.Equal(t, "temporal:7233", opts.HostPort)
	assert.Equal(t, "workflow", opts.Namespace)
	assert.Len(t, opts.Interceptors, 1)
	assert.Nil(t, opts.ConnectionOptions.TLS)
}

func TestTemporalDialOptions_RegisterNoTracingWhenExportIsOffWhateverTheRawVariableHolds(t *testing.T) {
	for _, endpoint := range []string{"", "none"} {
		t.Run("endpoint="+endpoint, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
			cfg := &config.Config{TemporalHostPort: "temporal:7233", TemporalNamespace: "workflow", OTELExporterEndpoint: endpoint}

			opts, err := temporalDialOptions(cfg)

			require.NoError(t, err)
			assert.Empty(t, opts.Interceptors)
		})
	}
}

func TestTemporalDialOptions_AMissingCertificateFileFailsTheStart(t *testing.T) {
	cfg := &config.Config{TemporalHostPort: "temporal:7233", TemporalNamespace: "workflow", TemporalTLSCertPath: "/nonexistent/tls.crt", TemporalTLSKeyPath: "/nonexistent/tls.key"}

	_, err := temporalDialOptions(cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "temporal client certificate")
}
