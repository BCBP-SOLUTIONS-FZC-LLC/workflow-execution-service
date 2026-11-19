package obstest_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestSum_ALabelKeyNoMetricOfTheFamilyCarries(t *testing.T) {
	obstest.Init(t)
	registerer := gincommon.MetricsRegisterer()
	const name = "workflow_execution_service_label_key_probe"
	probe := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: "probe"}, []string{"kind"})
	require.NoError(t, registerer.Register(probe))
	t.Cleanup(func() { registerer.Unregister(probe) })
	probe.WithLabelValues("a").Add(2)

	tests := []struct {
		name       string
		labels     map[string]string
		want       float64
		wantFailed bool
	}{
		{"a misspelt key fails the test", map[string]string{"knd": "a"}, 0, true},
		{"one unknown key among known ones fails the test", map[string]string{"kind": "a", "knd": "a"}, 0, true},
		{"a known key with a value nothing carries is zero", map[string]string{"kind": "b"}, 0, false},
		{"a known key and value is summed", map[string]string{"kind": "a"}, 2, false},
		{"no labels sums the family", nil, 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &fxbRecordingTB{TB: t}
			var got float64
			done := make(chan struct{})
			go func() {
				defer close(done)
				got = obstest.Sum(rec, name, tt.labels)
			}()
			<-done

			if tt.wantFailed {
				require.Len(t, rec.failed, 1)
				assert.Contains(t, rec.failed[0], "knd")
				return
			}
			assert.Empty(t, rec.failed)
			assert.Equal(t, tt.want, got)
		})
	}
}
