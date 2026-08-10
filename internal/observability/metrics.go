// Package observability centralizes this service's Prometheus metrics,
// OTel tracing-init helpers, and Temporal Search-Attribute helpers, per
// execution LLD §7.6 and §3.6. See metrics.go, tracing.go, and
// searchattributes.go for each concern.
//
// This package defines the instrumentation surface only. It does not call
// Register, does not initialize tracing, and does not upsert Search
// Attributes anywhere else in this codebase — those call sites belong to
// whichever task owns the business logic that needs them.
package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Buckets tuned to each histogram's own LLD §10.3 SLO p99 target, not
// Prometheus's stock exponential defaults — finer resolution near the
// threshold that actually gets alerted on (LLD §7.6's histogram_quantile
// alert rules). No SLO is stated for workflow_activity_duration_seconds, so
// it uses prometheus.DefBuckets.
var (
	taskSignalBuckets        = []float64{.01, .025, .05, .1, .15, .2, .25, .3, .4, .5, .75, 1, 2.5}
	instanceStartBuckets     = []float64{.05, .1, .25, .5, .75, 1, 1.5, 2, 3, 5, 10}
	delegationRerouteBuckets = []float64{.25, .5, 1, 2, 3, 4, 5, 6, 8, 10, 15, 20}
)

// Metric vars are nil until Register runs, matching iam-user-profile's
// internal/adapter/outbound/metrics convention: callers outside this
// package must nil-guard before use, since Register is invoked once at real
// process boot (a later task's composition-root job, not this one's).
var (
	RecordVersionConflictTotal *prometheus.CounterVec
	DBTxRetryTotal             prometheus.Counter
	InternalEventsIngestTotal  *prometheus.CounterVec

	DelegationRerouteDurationSeconds prometheus.Histogram
	TaskSignalDurationSeconds        *prometheus.HistogramVec
	InstanceStartDurationSeconds     prometheus.Histogram
	WorkflowActivityDurationSeconds  *prometheus.HistogramVec

	InstanceDegradedTotal *prometheus.CounterVec
	WorkerActiveQueues    prometheus.Gauge

	InternalEventsLastReceivedTimestamp *prometheus.GaugeVec
	RLSViolationsTotal                  *prometheus.CounterVec
	WorkflowReplayFailuresTotal         *prometheus.CounterVec
	InstanceDegradedCurrent             *prometheus.GaugeVec
	InstanceDegradedOldestAgeSeconds    *prometheus.GaugeVec
	WorkerQueueLastPollTimestamp        *prometheus.GaugeVec
	SLABreachesTotal                    *prometheus.CounterVec
	OldestReadyTaskAgeSeconds           *prometheus.GaugeVec
	UpstreamDependencyErrorsTotal       *prometheus.CounterVec

	WFCacheHitsTotal   prometheus.Counter
	WFCacheMissesTotal prometheus.Counter

	OutboxOldestUnpublishedAgeSeconds prometheus.Gauge
)

var registerOnce sync.Once

// Register constructs and registers every metric above against the default
// Prometheus registry. Safe to call more than once (e.g. from more than one
// test in this package) — only the first call has any effect.
//
// Deliberately out of scope: the DB-pool gauges LLD §7.6 says are "already
// registered for Definition Service, applied identically to both processes'
// pools" — those are platform-pgcommon's own collector and need a real
// *pgxpool.Pool to construct, which only exists once a composition root
// wires one up.
func Register() {
	registerOnce.Do(register)
}

func register() {
	RecordVersionConflictTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "record_version_conflict_total",
		Help: "Total optimistic-lock record_version conflicts, by table.",
	}, []string{"table"})

	DBTxRetryTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "db_tx_retry_total",
		Help: "Total transaction retries across the service.",
	})

	InternalEventsIngestTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "internal_events_ingest_total",
		Help: "Total internal event ingest calls, by event_type and result.",
	}, []string{"event_type", "result"})

	DelegationRerouteDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "delegation_reroute_duration_seconds",
		Help:    "Envelope-time to reroute-commit latency for DelegationStarted handling (LLD §6.7 SLO, p99 <= 4s).",
		Buckets: delegationRerouteBuckets,
	})

	TaskSignalDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "task_signal_duration_seconds",
		Help:    "HTTP-request-received to 202-returned latency for task claim/complete/defer (LLD §10.3 SLO, p99 <= 300ms).",
		Buckets: taskSignalBuckets,
	}, []string{"operation"})

	InstanceStartDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "instance_start_duration_seconds",
		Help:    "POST /instances received to 202-returned latency (LLD §10.3 SLO, p99 <= 1s).",
		Buckets: instanceStartBuckets,
	})

	WorkflowActivityDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "workflow_activity_duration_seconds",
		Help:    "Per-Activity execution latency, by activity_name and outcome.",
		Buckets: prometheus.DefBuckets,
	}, []string{"activity_name", "outcome"})

	InstanceDegradedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "instance_degraded_total",
		Help: "Total instances that entered DEGRADED, by tenant_id.",
	}, []string{"tenant_id"})

	WorkerActiveQueues = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "worker_active_queues",
		Help: "Live count of dynamically-registered worker.Worker instances.",
	})

	InternalEventsLastReceivedTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "internal_events_last_received_timestamp",
		Help: "Unix timestamp of the last successfully dispatched POST /internal/events call, by event_type.",
	}, []string{"event_type"})

	RLSViolationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rls_violations_total",
		Help: "Total RLS policy violations recorded by rls_check_tenant, by type.",
	}, []string{"type"})

	WorkflowReplayFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "workflow_replay_failures_total",
		Help: "Total Temporal workflow replay/non-determinism failures, by workflow_type.",
	}, []string{"workflow_type"})

	InstanceDegradedCurrent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instance_degraded_current",
		Help: "Live count of instances currently DEGRADED, by tenant_id.",
	}, []string{"tenant_id"})

	InstanceDegradedOldestAgeSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instance_degraded_oldest_age_seconds",
		Help: "Age in seconds of the longest-parked DEGRADED instance, by tenant_id.",
	}, []string{"tenant_id"})

	WorkerQueueLastPollTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_queue_last_poll_timestamp",
		Help: "Unix timestamp of the last successful poll on a tenant-isolated task queue.",
	}, []string{"queue"})

	SLABreachesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sla_breaches_total",
		Help: "Total workflow.task.sla-breached events, by tenant_id.",
	}, []string{"tenant_id"})

	OldestReadyTaskAgeSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "oldest_ready_task_age_seconds",
		Help: "Age in seconds of the oldest unclaimed READY task, by tenant_id.",
	}, []string{"tenant_id"})

	UpstreamDependencyErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "upstream_dependency_errors_total",
		Help: "Total upstream dependency call failures, by dependency.",
	}, []string{"dependency"})

	WFCacheHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wf_cache_hits_total",
		Help: "Total compiled-plan cache hits.",
	})

	WFCacheMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wf_cache_misses_total",
		Help: "Total compiled-plan cache misses.",
	})

	OutboxOldestUnpublishedAgeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_unpublished_age_seconds",
		Help: "Age in seconds of the oldest unpublished outbox_events row.",
	})

	prometheus.MustRegister(
		RecordVersionConflictTotal,
		DBTxRetryTotal,
		InternalEventsIngestTotal,
		DelegationRerouteDurationSeconds,
		TaskSignalDurationSeconds,
		InstanceStartDurationSeconds,
		WorkflowActivityDurationSeconds,
		InstanceDegradedTotal,
		WorkerActiveQueues,
		InternalEventsLastReceivedTimestamp,
		RLSViolationsTotal,
		WorkflowReplayFailuresTotal,
		InstanceDegradedCurrent,
		InstanceDegradedOldestAgeSeconds,
		WorkerQueueLastPollTimestamp,
		SLABreachesTotal,
		OldestReadyTaskAgeSeconds,
		UpstreamDependencyErrorsTotal,
		WFCacheHitsTotal,
		WFCacheMissesTotal,
		OutboxOldestUnpublishedAgeSeconds,
	)

	preInitializeClosedLabelSets()
}

// preInitializeClosedLabelSets pre-creates label-combination children for
// metrics whose *every* label's vocabulary is fixed by the LLD, so
// dashboards show 0 instead of "no data" immediately after deploy.
// internal_events_ingest_total is skipped despite result being closed
// ({ok, bad_payload, decode_failed, error}) because its other label,
// event_type, is open-ended — there's no meaningful value to pre-init it
// with. Metrics with any open-ended label (tenant_id, activity_name, table,
// queue, event_type, workflow_type) are skipped the same way.
func preInitializeClosedLabelSets() {
	for _, op := range []string{"claim", "complete", "defer"} {
		TaskSignalDurationSeconds.WithLabelValues(op)
	}
	for _, violationType := range []string{"missing_guc", "cross_tenant"} {
		RLSViolationsTotal.WithLabelValues(violationType)
	}
	for _, dep := range []string{"definition_service", "iam"} {
		UpstreamDependencyErrorsTotal.WithLabelValues(dep)
	}
}
