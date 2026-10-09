package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

const (
	Domain       = "workflow"
	serviceScope = "workflow-execution-service"
)

var (
	taskSignalBuckets        = []float64{.01, .025, .05, .1, .15, .2, .25, .3, .4, .5, .75, 1, 2.5}
	instanceStartBuckets     = []float64{.05, .1, .25, .5, .75, 1, 1.5, 2, 3, 5, 10}
	delegationRerouteBuckets = []float64{.25, .5, 1, 2, 3, 4, 5, 6, 8, 10, 15, 20}
	activityBuckets          = prometheus.DefBuckets
)

var (
	RecordVersionConflictTotal *prometheus.CounterVec
	InternalEventsIngestTotal  *prometheus.CounterVec

	DelegationRerouteDurationSeconds prometheus.Observer
	TaskSignalDurationSeconds        *prometheus.HistogramVec
	InstanceStartDurationSeconds     prometheus.Observer
	WorkflowActivityDurationSeconds  *prometheus.HistogramVec

	InstanceDegradedTotal prometheus.Counter
	WorkerActiveQueues    prometheus.Gauge

	InternalEventsLastReceivedTimestamp *prometheus.GaugeVec
	RLSViolationsTotal                  *prometheus.CounterVec
	WorkflowReplayFailuresTotal         *prometheus.CounterVec
	InstanceDegradedCurrent             prometheus.Gauge
	InstanceDegradedOldestAgeSeconds    prometheus.Gauge
	WorkerQueueLastPollTimestamp        *prometheus.GaugeVec
	SLABreachesTotal                    prometheus.Counter
	DelegationScopeUnhandledTotal       *prometheus.CounterVec
	ConnectorDispatchDeadLetteredTotal  *prometheus.CounterVec
	OldestReadyTaskAgeSeconds           prometheus.Gauge
	UpstreamDependencyErrorsTotal       *prometheus.CounterVec

	WFCacheHitsTotal   prometheus.Counter
	WFCacheMissesTotal prometheus.Counter
)

var registerOnce sync.Once

func Register() {
	registerOnce.Do(register)
}

func must[C any](c C, err error) C {
	if err != nil {
		panic("metrics: register service collector: " + err.Error())
	}
	return c
}

func register() {
	RecordVersionConflictTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_record_version_conflict_total",
		"Total optimistic-lock record_version conflicts, by table.", "table"))
	InternalEventsIngestTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_internal_events_ingest_total",
		"Total internal event ingest calls, by event_type and result.", "event_type", "result"))
	DelegationScopeUnhandledTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_delegation_scope_unhandled_total",
		"Delegations arriving with a scope this service has no case for, by scope. Still matched against subject_id.", "scope"))
	ConnectorDispatchDeadLetteredTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_connector_dispatch_dead_lettered_total",
		"Connector stream entries moved to the dead-letter stream after exceeding their delivery cap, by connector_type.", "connector_type"))
	DelegationRerouteDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_delegation_reroute_duration_seconds",
		"Envelope-time to reroute-commit latency for DelegationStarted handling (LLD §6.7 SLO, p99 <= 4s).", delegationRerouteBuckets)).WithLabelValues()
	TaskSignalDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_task_signal_duration_seconds",
		"HTTP-request-received to 202-returned latency for task claim/complete/defer (LLD §10.3 SLO, p99 <= 300ms).", taskSignalBuckets, "operation"))
	InstanceStartDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_instance_start_duration_seconds",
		"POST /instances received to 202-returned latency (LLD §10.3 SLO, p99 <= 1s).", instanceStartBuckets)).WithLabelValues()
	WorkflowActivityDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_workflow_activity_duration_seconds",
		"Per-Activity execution latency, by activity_name and outcome.", activityBuckets, "activity_name", "outcome"))
	InstanceDegradedTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_instance_degraded_total",
		"Total instances that entered DEGRADED."))
	WorkerActiveQueues = must(gincommon.NewGauge(nil,
		"workflow_execution_service_worker_active_queues",
		"Live count of dynamically-registered worker.Worker instances."))
	InternalEventsLastReceivedTimestamp = must(gincommon.NewGaugeVec(nil,
		"workflow_execution_service_internal_events_last_received_timestamp",
		"Unix timestamp of the last successfully dispatched POST /internal/events call, by event_type.", "event_type"))
	RLSViolationsTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_rls_violations_total",
		"Sampled row-level-security violations recorded in rls_violation_log, by violation_type.", "violation_type"))
	WorkflowReplayFailuresTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_workflow_replay_failures_total",
		"Total Temporal workflow replay/non-determinism failures, by workflow_type.", "workflow_type"))
	InstanceDegradedCurrent = must(gincommon.NewGauge(nil,
		"workflow_execution_service_instance_degraded_current",
		"Live count of instances currently DEGRADED."))
	InstanceDegradedOldestAgeSeconds = must(gincommon.NewGauge(nil,
		"workflow_execution_service_instance_degraded_oldest_age_seconds",
		"Age in seconds of the longest-parked DEGRADED instance."))
	WorkerQueueLastPollTimestamp = must(gincommon.NewGaugeVec(nil,
		"workflow_execution_service_worker_queue_last_poll_timestamp",
		"Unix timestamp of the last successful poll on a tenant-isolated task queue.", "queue"))
	SLABreachesTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_sla_breaches_total",
		"Total workflow.task.sla-breached events."))
	OldestReadyTaskAgeSeconds = must(gincommon.NewGauge(nil,
		"workflow_execution_service_oldest_ready_task_age_seconds",
		"Age in seconds of the oldest unclaimed READY task."))
	UpstreamDependencyErrorsTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_upstream_dependency_errors_total",
		"Total upstream dependency call failures, by dependency.", "dependency"))
	WFCacheHitsTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_wf_cache_hits_total",
		"Total compiled-plan cache hits."))
	WFCacheMissesTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_wf_cache_misses_total",
		"Total compiled-plan cache misses."))

	preInitializeClosedLabelSets()
}

func preInitializeClosedLabelSets() {
	for _, op := range []string{"claim", "complete", "defer"} {
		TaskSignalDurationSeconds.WithLabelValues(op)
	}
	for _, violationType := range []string{"missing_or_invalid_guc", "cross_tenant_access"} {
		RLSViolationsTotal.WithLabelValues(violationType)
	}
	for _, dep := range []string{"definition_service", "iam"} {
		UpstreamDependencyErrorsTotal.WithLabelValues(dep)
	}
}

func IncCounter(c prometheus.Counter) {
	if c != nil {
		c.Inc()
	}
}

func IncCounterVec(cv *prometheus.CounterVec, labelValues ...string) {
	if cv != nil {
		cv.WithLabelValues(labelValues...).Inc()
	}
}

func ObserveHistogram(h prometheus.Observer, seconds float64) {
	if h != nil {
		h.Observe(seconds)
	}
}

func ObserveHistogramVec(hv *prometheus.HistogramVec, seconds float64, labelValues ...string) {
	if hv != nil {
		hv.WithLabelValues(labelValues...).Observe(seconds)
	}
}

func SetGauge(g prometheus.Gauge, v float64) {
	if g != nil {
		g.Set(v)
	}
}

func AddRLSViolations(violationType string, n float64) {
	if RLSViolationsTotal != nil {
		RLSViolationsTotal.WithLabelValues(violationType).Add(n)
	}
}

func RecordOperationalStats(degraded int64, oldestDegradedAgeSeconds, oldestReadyTaskAgeSeconds float64) {
	SetGauge(InstanceDegradedCurrent, float64(degraded))
	SetGauge(InstanceDegradedOldestAgeSeconds, oldestDegradedAgeSeconds)
	SetGauge(OldestReadyTaskAgeSeconds, oldestReadyTaskAgeSeconds)
}
