package obstest_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestSum_CounterVec_AddsTheSeriesMatchingTheGivenLabels(t *testing.T) {
	obstest.Init(t)
	registerer := gincommon.MetricsRegisterer()
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_execution_service_w10b_probe_total", Help: "probe",
	}, []string{"kind"})
	require.NoError(t, registerer.Register(vec))
	t.Cleanup(func() { registerer.Unregister(vec) })
	vec.WithLabelValues("a").Add(2)
	vec.WithLabelValues("b").Add(5)

	const name = "workflow_execution_service_w10b_probe_total"
	assert.InDelta(t, 2, obstest.Sum(t, name, map[string]string{"kind": "a"}), 0)
	assert.InDelta(t, 5, obstest.Sum(t, name, map[string]string{"kind": "b"}), 0)
	assert.InDelta(t, 7, obstest.Sum(t, name, nil), 0, "no labels sums every series")
	assert.InDelta(t, 0, obstest.Sum(t, name, map[string]string{"kind": "absent"}), 0)
}
