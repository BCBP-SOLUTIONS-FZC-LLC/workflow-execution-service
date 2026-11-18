package obstest_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type fxbRecordingTB struct {
	testing.TB
	mu     sync.Mutex
	failed []string
}

func (r *fxbRecordingTB) Helper() {}

func (r *fxbRecordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.failed = append(r.failed, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func fxbSumRecorded(tb *fxbRecordingTB, name string) float64 {
	var got float64
	done := make(chan struct{})
	go func() {
		defer close(done)
		got = obstest.Sum(tb, name, nil)
	}()
	<-done
	return got
}

func TestSum_ASummaryMetricFailsTheTestInsteadOfReturningZero(t *testing.T) {
	obstest.Init(t)
	registerer := gincommon.MetricsRegisterer()
	const name = "workflow_execution_service_fxb_summary_probe"
	summary := prometheus.NewSummary(prometheus.SummaryOpts{Name: name, Help: "probe"})
	require.NoError(t, registerer.Register(summary))
	t.Cleanup(func() { registerer.Unregister(summary) })
	summary.Observe(3)

	rec := &fxbRecordingTB{TB: t}
	fxbSumRecorded(rec, name)

	require.Len(t, rec.failed, 1)
	require.Contains(t, rec.failed[0], name)
}

func TestSum_AnUntypedMetricFailsTheTestInsteadOfReturningZero(t *testing.T) {
	obstest.Init(t)
	registerer := gincommon.MetricsRegisterer()
	const name = "workflow_execution_service_fxb_untyped_probe"
	untyped := prometheus.NewUntypedFunc(prometheus.UntypedOpts{Name: name, Help: "probe"}, func() float64 { return 4 })
	require.NoError(t, registerer.Register(untyped))
	t.Cleanup(func() { registerer.Unregister(untyped) })

	rec := &fxbRecordingTB{TB: t}
	fxbSumRecorded(rec, name)

	require.Len(t, rec.failed, 1)
	require.Contains(t, rec.failed[0], name)
}
