package observability_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// TestRegisterPGMetrics_IsIdempotentAndScrapable exercises pgmetrics.
// pgcommon_slow_query_total is a plain Counter (no labels), so — unlike the
// *Vec collectors this package documents elsewhere — it emits a sample on
// every scrape immediately after registration, with no WithLabelValues call
// needed first.
func TestRegisterPGMetrics_IsIdempotentAndScrapable(t *testing.T) {
	require.NotPanics(t, func() {
		observability.RegisterPGMetrics("execution-service-test", "test")
		observability.RegisterPGMetrics("execution-service-test", "test")
	})

	body := scrape(t)
	require.Contains(t, body, "pgcommon_slow_query_total")
}
