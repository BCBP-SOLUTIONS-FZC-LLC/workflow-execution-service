package obstest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const w30bFailuresMetric = "workflow_execution_service_connector_task_failures_total"

func TestInit_CalledAgain_ReturnsNormallyAndKeepsEarlierCounts(t *testing.T) {
	obstest.Init(t)
	collector := observability.ConnectorTaskFailuresTotal
	labels := map[string]string{"connector_type": "w30b-init"}
	before := obstest.Sum(t, w30bFailuresMetric, labels)
	observability.IncCounterVec(observability.ConnectorTaskFailuresTotal, "w30b-init", "s3", "transient", "false")
	counted := obstest.Sum(t, w30bFailuresMetric, labels)
	require.InDelta(t, before+1, counted, 0)

	require.NotPanics(t, func() { obstest.Init(t) })
	require.NotPanics(t, func() { obstest.Init(t) })

	assert.InDelta(t, counted, obstest.Sum(t, w30bFailuresMetric, labels), 0, "a later Init does not reset the counters")
	assert.Same(t, collector, observability.ConnectorTaskFailuresTotal, "a later Init keeps the registered collector instead of building a new one")
	observability.IncCounterVec(observability.ConnectorTaskFailuresTotal, "w30b-init", "s3", "transient", "false")
	assert.InDelta(t, counted+1, obstest.Sum(t, w30bFailuresMetric, labels), 0, "the collector the code increments is the one Sum reads")
}

func TestSum_PartialLabelSet_AddsEverySeriesThatContainsTheGivenPairs(t *testing.T) {
	obstest.Init(t)
	fail := observability.ConnectorTaskFailuresTotal
	byType := map[string]string{"connector_type": "w30b-subset"}
	byTypeAndClass := map[string]string{"connector_type": "w30b-subset", "class": "transient"}
	exact := map[string]string{"connector_type": "w30b-subset", "provider": "s3", "class": "transient", "retried": "true"}
	otherType := map[string]string{"connector_type": "w30b-subset-other"}
	beforeType := obstest.Sum(t, w30bFailuresMetric, byType)
	beforeTypeClass := obstest.Sum(t, w30bFailuresMetric, byTypeAndClass)
	beforeExact := obstest.Sum(t, w30bFailuresMetric, exact)
	beforeOther := obstest.Sum(t, w30bFailuresMetric, otherType)

	observability.IncCounterVec(fail, "w30b-subset", "s3", "transient", "true")
	observability.IncCounterVec(fail, "w30b-subset", "s3", "transient", "true")
	observability.IncCounterVec(fail, "w30b-subset", "gcs", "transient", "false")
	observability.IncCounterVec(fail, "w30b-subset", "gcs", "permanent", "false")
	observability.IncCounterVec(fail, "w30b-subset-other", "s3", "transient", "true")

	assert.InDelta(t, beforeType+4, obstest.Sum(t, w30bFailuresMetric, byType), 0, "connector_type alone sums over provider, class and retried")
	assert.InDelta(t, beforeTypeClass+3, obstest.Sum(t, w30bFailuresMetric, byTypeAndClass), 0, "two pairs still ignore the unspecified labels")
	assert.InDelta(t, beforeExact+2, obstest.Sum(t, w30bFailuresMetric, exact), 0, "a full label set matches one series")
	assert.InDelta(t, beforeOther+1, obstest.Sum(t, w30bFailuresMetric, otherType), 0, "a series of another connector_type is not added")
}
