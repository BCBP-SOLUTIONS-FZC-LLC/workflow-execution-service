//go:build e2e

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// The stream consumer name identifies this replica in the consumer group, so a
// configured name is kept and an unset one becomes the host name.
func TestBuildDeps_StreamConsumerName_ConfiguredValueWinsAndUnsetUsesTheHostName(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)
	valkeyAddr := fixtures.NewTestValkey(t)
	aliasSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":1,"restCall":[],"sqlQuery":[]}`))
	}))
	t.Cleanup(aliasSrv.Close)
	host, err := os.Hostname()
	require.NoError(t, err)
	require.NotEmpty(t, host)

	build := func(t *testing.T, configuredName string) *deps {
		t.Helper()
		cfg := &config.Config{
			DatabaseURL:                  dsn,
			ConnectorRetryMaxAttempts:    1,
			ValkeyAddr:                   valkeyAddr,
			ValkeyDialTimeout:            5 * time.Second,
			ValkeyReadTimeout:            5 * time.Second,
			ValkeyWriteTimeout:           5 * time.Second,
			ConnectorStreamKey:           "connector-tasks-consumer-name",
			ConnectorStreamGroup:         "connector-worker-consumer-name",
			ConnectorStreamConsumerName:  configuredName,
			ConnectorStreamBlockTimeout:  time.Second,
			ConnectorStreamClaimMinIdle:  30 * time.Second,
			ConnectorStreamBatchSize:     10,
			DefinitionServiceURL:         aliasSrv.URL,
			ConnectorAliasFetchTimeout:   5 * time.Second,
			OpenBaoAddr:                  "http://unused.invalid",
			OpenBaoMount:                 "secret",
			OpenBaoTimeout:               5 * time.Second,
			ExecutionServiceInternalAddr: "http://unused.invalid",
			ConnectorCompletionTimeout:   5 * time.Second,
		}
		d, cleanup, err := buildDeps(cfg, noopLogger{})
		require.NoError(t, err)
		t.Cleanup(cleanup)
		return d
	}

	t.Run("a configured name is used as is", func(t *testing.T) {
		assert.Equal(t, "replica-from-config", build(t, "replica-from-config").consumerName)
	})
	t.Run("an unset name becomes the host name", func(t *testing.T) {
		assert.Equal(t, host, build(t, "").consumerName)
	})
}
