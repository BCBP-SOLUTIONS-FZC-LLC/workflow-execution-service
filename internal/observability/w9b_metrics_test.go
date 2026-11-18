package observability_test

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// w9bBucketBounds returns the finite bucket upper bounds of the first series
// of a histogram family, in the order a scrape reports them.
func w9bBucketBounds(t *testing.T, family string) []float64 {
	t.Helper()
	initIdentity(t)
	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	for _, mf := range gathered {
		if mf.GetName() != family {
			continue
		}
		require.NotEmpty(t, mf.GetMetric(), "%s has no series", family)
		var bounds []float64
		for _, b := range mf.GetMetric()[0].GetHistogram().GetBucket() {
			bounds = append(bounds, b.GetUpperBound())
		}
		return bounds
	}
	require.Failf(t, "histogram missing", "%s is not in the scrape", family)
	return nil
}

func TestRegister_HistogramBuckets_CoverTheirSLOWindowAndHaveTheSLOBoundary(t *testing.T) {
	cases := []struct {
		name     string
		family   string
		lowest   float64
		highest  float64
		boundary float64
	}{
		{
			name:     "task signal from .01 to 2.5 with .3 as a boundary",
			family:   "workflow_execution_service_task_signal_duration_seconds",
			lowest:   .01,
			highest:  2.5,
			boundary: .3,
		},
		{
			name:     "instance start from .05 to 10 with 1 as a boundary",
			family:   "workflow_execution_service_instance_start_duration_seconds",
			lowest:   .05,
			highest:  10,
			boundary: 1,
		},
		{
			name:     "delegation reroute from .25 to 20 with 4 as a boundary",
			family:   "workflow_execution_service_delegation_reroute_duration_seconds",
			lowest:   .25,
			highest:  20,
			boundary: 4,
		},
		{
			name:    "entitlements call from .005 to 10",
			family:  "workflow_execution_service_entitlements_call_duration_seconds",
			lowest:  .005,
			highest: 10,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			initIdentity(t)

			bounds := w9bBucketBounds(t, tc.family)

			require.NotEmpty(t, bounds)
			assert.Equal(t, tc.lowest, bounds[0], "lowest bucket")
			assert.Equal(t, tc.highest, bounds[len(bounds)-1], "highest finite bucket")
			if tc.boundary != 0 {
				assert.Contains(t, bounds, tc.boundary, "the SLO threshold must be a bucket boundary")
			}
			assert.True(t, sort.Float64sAreSorted(bounds))
		})
	}
}

func TestCounterHelpers_NegativeAmount_PanicsAndRecordsNothing(t *testing.T) {
	cases := []struct {
		name   string
		series func() prometheus.Counter
		add    func()
	}{
		{
			name: "rls violations",
			series: func() prometheus.Counter {
				return observability.RLSViolationsTotal.WithLabelValues("cross_tenant_access")
			},
			add: func() { observability.AddRLSViolations("cross_tenant_access", -1) },
		},
		{
			name: "retention rows deleted",
			series: func() prometheus.Counter {
				return observability.RetentionRowsDeletedTotal.WithLabelValues("w9b_negative_probe")
			},
			add: func() { observability.AddRetentionRowsDeleted("w9b_negative_probe", -1) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			initIdentity(t)
			before := testutil.ToFloat64(tc.series())

			assert.Panics(t, tc.add, "a Prometheus counter cannot decrease")

			assert.Equal(t, before, testutil.ToFloat64(tc.series()))
		})
	}
}

func TestRecordOperationalStats_ConcurrentCalls_LeaveTheSeriesOfOneWholeCall(t *testing.T) {
	initIdentity(t)
	const callers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id := strconv.Itoa(i)
			observability.RecordOperationalStats(int64(i), float64(i), float64(i), map[string]int64{
				"w9b-a-" + id: int64(i),
				"w9b-b-" + id: int64(i),
			})
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "concurrent RecordOperationalStats calls did not finish")
	}

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	got := map[string]float64{}
	for _, mf := range gathered {
		if mf.GetName() != "workflow_execution_service_instances_by_queue" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "queue" {
					got[lp.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}

	require.Len(t, got, 2, "the queues of earlier calls must have been dropped: %v", got)
	var winner string
	for queue, value := range got {
		suffix := queue[strings.LastIndex(queue, "-")+1:]
		if winner == "" {
			winner = suffix
		}
		assert.Equal(t, winner, suffix, "both series come from the same call")
		assert.Equal(t, suffix, strconv.FormatFloat(value, 'f', -1, 64), "the value is the one that call wrote")
	}
	_, hasA := got["w9b-a-"+winner]
	_, hasB := got["w9b-b-"+winner]
	assert.True(t, hasA && hasB, "the series are the two queues of the winning call: %v", got)
}

func TestRegister_ActivityDurationHistogramExistsWithItsLabels(t *testing.T) {
	initIdentity(t)
	observability.ObserveHistogramVec(observability.WorkflowActivityDurationSeconds, 0.01, "w9bLabelProbe", "success")

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	found := false
	for _, mf := range gathered {
		if mf.GetName() != "workflow_execution_service_workflow_activity_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["activity_name"] == "w9bLabelProbe" && labels["outcome"] == "success" {
				found = true
				assert.NotEmpty(t, m.GetHistogram().GetBucket())
			}
		}
	}
	assert.True(t, found, "the series carries the activity_name and outcome labels")
}
