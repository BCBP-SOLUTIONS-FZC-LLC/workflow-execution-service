package obstest

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func Init(tb testing.TB) {
	tb.Helper()
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		ServiceName:  "workflow-execution-service",
		BuildVersion: "test",
		Domain:       observability.Domain,
		Environment:  "test",
	})
	observability.Register()
}

func Sum(tb testing.TB, name string, labels map[string]string) float64 {
	tb.Helper()
	families, err := gincommon.MetricsGatherer().Gather()
	if err != nil {
		tb.Fatalf("gather: %v", err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matches(m, labels) {
				total += value(f.GetType(), m)
			}
		}
	}
	return total
}

func matches(m *dto.Metric, labels map[string]string) bool {
	for k, v := range labels {
		found := false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == k && lp.GetValue() == v {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func value(t dto.MetricType, m *dto.Metric) float64 {
	if t == dto.MetricType_COUNTER {
		return m.GetCounter().GetValue()
	}
	if t == dto.MetricType_GAUGE {
		return m.GetGauge().GetValue()
	}
	if t == dto.MetricType_HISTOGRAM {
		return float64(m.GetHistogram().GetSampleCount())
	}
	return 0
}
