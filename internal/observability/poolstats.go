package observability

import (
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgmetrics"
)

// RegisterPoolStats exposes pool's connection-pool gauges (idle/active/max
// conns, wait counts) on every Prometheus scrape. label must be distinct per
// pool in a process — pgmetrics.NewPoolStatsCollector's descriptors carry
// only a service const label with no pool-role dimension, so two pools
// sharing a label collide at registration. Call once per pool, right after
// it's constructed.
func RegisterPoolStats(pool *pgcommon.Pool, label string) {
	gincommon.MetricsRegisterer().MustRegister(pgmetrics.NewPoolStatsCollector(pool, label))
}
