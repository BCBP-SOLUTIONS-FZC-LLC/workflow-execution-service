package obstest

import (
	"slices"
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
		requireKnownLabelKeys(tb, f, labels)
		for _, m := range f.GetMetric() {
			if matches(m, labels) {
				total += value(tb, f.GetName(), f.GetType(), m)
			}
		}
	}
	return total
}

func requireKnownLabelKeys(tb testing.TB, f *dto.MetricFamily, labels map[string]string) {
	tb.Helper()
	if len(f.GetMetric()) == 0 {
		return
	}
	for k := range labels {
		known := slices.ContainsFunc(f.GetMetric(), func(m *dto.Metric) bool {
			return slices.ContainsFunc(m.GetLabel(), func(lp *dto.LabelPair) bool { return lp.GetName() == k })
		})
		if !known {
			tb.Fatalf("obstest: no metric of %s carries the label %q", f.GetName(), k)
		}
	}
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

func value(tb testing.TB, name string, t dto.MetricType, m *dto.Metric) float64 {
	tb.Helper()
	if t == dto.MetricType_COUNTER {
		return m.GetCounter().GetValue()
	}
	if t == dto.MetricType_GAUGE {
		return m.GetGauge().GetValue()
	}
	if t == dto.MetricType_HISTOGRAM {
		return float64(m.GetHistogram().GetSampleCount())
	}
	tb.Fatalf("obstest: metric %s has type %s, which Sum does not support", name, t)
	return 0
}
