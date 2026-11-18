package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// The longest activity StartToClose timeout in internal/workflow is 30s
// (the database-write options); an attempt that runs to its timeout must land
// in a finite bucket, or p99 for slow activities is unreadable.
const fxdLongestActivityTimeoutSeconds = 30.0

func TestRegister_ActivityDurationBuckets_ReachTheLongestActivityTimeout(t *testing.T) {
	initIdentity(t)
	observability.ObserveHistogramVec(observability.WorkflowActivityDurationSeconds, 0.01, "fxdBucketProbe", "success")
	bounds := w9bBucketBounds(t, "workflow_execution_service_workflow_activity_duration_seconds")
	require.NotEmpty(t, bounds)
	assert.Contains(t, bounds, fxdLongestActivityTimeoutSeconds)
	assert.GreaterOrEqual(t, bounds[len(bounds)-1], fxdLongestActivityTimeoutSeconds)
}
