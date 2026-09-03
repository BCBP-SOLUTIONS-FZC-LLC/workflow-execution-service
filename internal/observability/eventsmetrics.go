package observability

import (
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

var registerEventsMetricsOnce sync.Once

// RegisterEventsMetrics activates platform-events' own built-in collectors
// (events_published_total, outbox_pending_total, outbox_dead_letters_total,
// and the rest of its Prometheus surface) against the same registerer this
// package's Register uses, instead of the never-called events.Init leaving
// them permanently no-op — the same gap RegisterPGMetrics closed for
// pgcommon. Call once per process, alongside Register(); safe to call more
// than once — only the first call has any effect.
func RegisterEventsMetrics(serviceName, buildVersion string) {
	registerEventsMetricsOnce.Do(func() {
		events.InitWithRegisterer(serviceName, buildVersion, gincommon.MetricsRegisterer())
	})
}
