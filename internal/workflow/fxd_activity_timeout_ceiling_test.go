package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The activity-duration histogram's top bucket is sized to this ceiling
// (internal/observability); raising a timeout here means raising the buckets there.
func TestActivityOptions_StartToCloseStaysWithinTheMetricBuckets(t *testing.T) {
	assert.LessOrEqual(t, dbWriteActivityOptions.StartToCloseTimeout, 30*time.Second)
	assert.LessOrEqual(t, externalCallActivityOptions.StartToCloseTimeout, 30*time.Second)
}
