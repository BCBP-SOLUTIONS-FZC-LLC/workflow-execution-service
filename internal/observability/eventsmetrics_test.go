package observability_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestRegisterEventsMetrics_IsIdempotentAndScrapable(t *testing.T) {
	require.NotPanics(t, func() {
		observability.RegisterEventsMetrics("execution-service-test", "test")
		observability.RegisterEventsMetrics("execution-service-test", "test")
	})

	body := scrape(t)
	require.Contains(t, body, "platform_events_build_info")
}
