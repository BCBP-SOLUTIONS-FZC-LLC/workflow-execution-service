package observability

import (
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgmetrics"
)

var registerPGMetricsOnce sync.Once

// RegisterPGMetrics activates platform-pgcommon's own built-in collectors
// (pgcommon_query_total, pgcommon_query_duration_seconds,
// pgcommon_pool_acquire_*, pgcommon_slow_query_total, pgcommon_retry_total,
// pgcommon_retry_exhausted_total) against the same registerer this
// package's Register uses, instead of the never-called pgmetrics.Init
// leaving them permanently no-op. Call once per process, alongside
// Register(); safe to call more than once — only the first call has any
// effect.
func RegisterPGMetrics(serviceName, buildVersion string) {
	registerPGMetricsOnce.Do(func() {
		pgmetrics.InitWithRegisterer(serviceName, buildVersion, gincommon.MetricsRegisterer())
	})
}
