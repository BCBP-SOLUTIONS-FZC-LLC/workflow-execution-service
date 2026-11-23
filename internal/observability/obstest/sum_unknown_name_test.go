package obstest_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestSum_AMetricNameNoSeriesCarries(t *testing.T) {
	obstest.Init(t)
	registerer := gincommon.MetricsRegisterer()
	const touched = "workflow_execution_service_name_probe_touched_total"
	const untouched = "workflow_execution_service_name_probe_untouched_total"
	touchedVec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: touched, Help: "probe"}, []string{"kind"})
	untouchedVec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: untouched, Help: "probe"}, []string{"kind"})
	require.NoError(t, registerer.Register(touchedVec))
	require.NoError(t, registerer.Register(untouchedVec))
	t.Cleanup(func() {
		registerer.Unregister(touchedVec)
		registerer.Unregister(untouchedVec)
	})
	touchedVec.WithLabelValues("a").Add(3)

	tests := []struct {
		name       string
		metric     string
		want       float64
		wantFailed bool
	}{
		{"a name that is not registered fails the test", "workflow_execution_service_name_probe_typo_total", 0, true},
		{"a labelled metric no code has touched fails the test", untouched, 0, true},
		{"a metric with a series is summed", touched, 3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &fxbRecordingTB{TB: t}
			var got float64
			done := make(chan struct{})
			go func() {
				defer close(done)
				got = obstest.Sum(rec, tt.metric, nil)
			}()
			<-done

			if tt.wantFailed {
				require.Len(t, rec.failed, 1)
				assert.Contains(t, rec.failed[0], tt.metric)
				return
			}
			assert.Empty(t, rec.failed)
			assert.Equal(t, tt.want, got)
		})
	}
}
