package observability_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestOldestReadyConnectorTaskAgeGauge_IsScrapedAndSettable(t *testing.T) {
	initIdentity(t)
	require.NotNil(t, observability.OldestReadyConnectorTaskAgeSeconds)

	observability.SetGauge(observability.OldestReadyConnectorTaskAgeSeconds, 2400)

	require.Contains(t, scrape(t), "workflow_execution_service_oldest_ready_connector_task_age_seconds")
	require.Equal(t, 2400.0, testutil.ToFloat64(observability.OldestReadyConnectorTaskAgeSeconds))
}
