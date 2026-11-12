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
	entitlementsCallBuckets  = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
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
	InstancesByQueue                    *prometheus.GaugeVec
	SLABreachesTotal                    prometheus.Counter
	DelegationScopeUnhandledTotal       *prometheus.CounterVec
	RetentionRowsDeletedTotal           *prometheus.CounterVec
	TenantPurgeFailuresTotal            prometheus.Counter
	ConnectorDispatchDeadLetteredTotal  *prometheus.CounterVec
	ConnectorTaskFailuresTotal          *prometheus.CounterVec
	OldestReadyTaskAgeSeconds           prometheus.Gauge
	UpstreamDependencyErrorsTotal       *prometheus.CounterVec
	UserPauseFailuresTotal              *prometheus.CounterVec
	InternalAuthRejectedTotal           *prometheus.CounterVec
	ConnectorAliasRegistryAgeSeconds    prometheus.Gauge

	EntitlementsCallDurationSeconds    *prometheus.HistogramVec
	EntitlementsCallErrorsTotal        *prometheus.CounterVec
	EntitlementsStaleServedTotal       prometheus.Counter
	EntitlementsOldestServedAgeSeconds prometheus.Gauge
	EntitlementsReadPanicsTotal        prometheus.Counter

	WFCacheHitsTotal   prometheus.Counter
	WFCacheMissesTotal prometheus.Counter
)

var registerOnce sync.Once

func Register() {
	registerOnce.Do(registerServiceMetrics)
}

func must[C any](c C, err error) C {
	if err != nil {
		panic("metrics: register service collector: " + err.Error())
	}
	return c
}

func registerServiceMetrics() {
	RecordVersionConflictTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_record_version_conflict_total",
		"Total optimistic-lock record_version conflicts, by table.", "table"))
	InternalEventsIngestTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_internal_events_ingest_total",
		"Total internal event ingest calls, by event_type and result.", "event_type", "result"))
	DelegationScopeUnhandledTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_delegation_scope_unhandled_total",
		"Delegations arriving with a scope this service has no case for, by scope. They match no assignment.", "scope"))
	RetentionRowsDeletedTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_retention_rows_deleted_total",
		"Rows the retention sweeps deleted, by table (outbox_events, processed_events, or a tenant's purged tables).", "table"))
	TenantPurgeFailuresTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_tenant_purge_failures_total",
		"Tenant purges that failed part-way and will be retried on the next sweep."))
	ConnectorDispatchDeadLetteredTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_connector_dispatch_dead_lettered_total",
		"Connector stream entries moved to the dead-letter stream after exceeding their delivery cap, by connector_type.", "connector_type"))
	ConnectorTaskFailuresTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_connector_task_failures_total",
		"Failed connector calls, by connector_type, provider, error class (transient, permanent or unknown) and whether the call was retried.", "connector_type", "provider", "class", "retried"))
	DelegationRerouteDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_delegation_reroute_duration_seconds",
		"Envelope-time to reroute-commit latency for DelegationStarted handling (LLD §6.7 SLO, p99 <= 4s).", delegationRerouteBuckets)).WithLabelValues()
	TaskSignalDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_task_signal_duration_seconds",
		"HTTP-request-received to 202-returned latency for task claim/complete/defer (LLD §7.6 SLO, p99 <= 300ms).", taskSignalBuckets, "operation"))
	InstanceStartDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_instance_start_duration_seconds",
		"POST /instances received to 201-returned latency (LLD §7.6 SLO, p99 <= 1s).", instanceStartBuckets)).WithLabelValues()
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
		"Unix time of the last topology poll in which this process served the task queue (set on every poll for every queue it runs a worker for, not a measure of the Temporal long poll itself).", "queue"))
	InstancesByQueue = must(gincommon.NewGaugeVec(nil,
		"workflow_execution_service_instances_by_queue",
		"Non-terminal instances (RUNNING, PAUSED or DEGRADED) per task queue, sampled by the API process. A queue with none is absent.",
		"queue"))
	SLABreachesTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_sla_breaches_total",
		"Total workflow.task.sla-breached events."))
	OldestReadyTaskAgeSeconds = must(gincommon.NewGauge(nil,
		"workflow_execution_service_oldest_ready_task_age_seconds",
		"Age in seconds of the oldest unclaimed READY task."))
	UpstreamDependencyErrorsTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_upstream_dependency_errors_total",
		"Total upstream dependency call failures, by dependency.", "dependency"))
	UserPauseFailuresTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_user_pause_failures_total",
		"Instances the membership-revocation pause could not pause, by stage (read_task, read_instance or signal). The call answers 503 so the caller retries.", "stage"))
	InternalAuthRejectedTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_internal_auth_rejected_total",
		"Internal-route requests refused for lacking the system caller identity, by reason (missing_user or missing_role).", "reason"))
	ConnectorAliasRegistryAgeSeconds = must(gincommon.NewGauge(nil,
		"workflow_execution_service_connector_alias_registry_age_seconds",
		"Seconds since the connector worker last applied a fresh alias registry, updated on every refresh tick (a failed or empty refresh lets it grow)."))
	EntitlementsCallDurationSeconds = must(gincommon.NewHistogramVec(nil,
		"workflow_execution_service_entitlements_call_duration_seconds",
		"Latency of one Entitlements feature-flag read, by outcome (success or error).", entitlementsCallBuckets, "outcome"))
	EntitlementsCallErrorsTotal = must(gincommon.NewCounterVec(nil,
		"workflow_execution_service_entitlements_call_errors_total",
		"Failed Entitlements feature-flag reads, by reason (transport, status or response).", "reason"))
	EntitlementsStaleServedTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_entitlements_stale_served_total",
		"Feature-flag answers served from a held set older than its TTL while a refresh runs or backs off."))
	EntitlementsOldestServedAgeSeconds = must(gincommon.NewGauge(nil,
		"workflow_execution_service_entitlements_oldest_served_age_seconds",
		"Age in seconds of the oldest held feature-flag set served past its TTL, over the last TTL window; 0 when none was."))
	EntitlementsReadPanicsTotal = must(gincommon.NewCounter(nil,
		"workflow_execution_service_entitlements_read_panics_total",
		"Entitlements reads that panicked and were recovered; each counts as a failed read."))
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
	for _, stage := range []string{"read_task", "read_instance", "signal"} {
		UserPauseFailuresTotal.WithLabelValues(stage)
	}
	for _, reason := range []string{"missing_user", "missing_role"} {
		InternalAuthRejectedTotal.WithLabelValues(reason)
	}
	for _, outcome := range []string{"success", "error"} {
		EntitlementsCallDurationSeconds.WithLabelValues(outcome)
	}
	for _, reason := range []string{"transport", "status", "response"} {
		EntitlementsCallErrorsTotal.WithLabelValues(reason)
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

func AddRetentionRowsDeleted(table string, n int64) {
	if RetentionRowsDeletedTotal != nil {
		RetentionRowsDeletedTotal.WithLabelValues(table).Add(float64(n))
	}
}

func RecordOperationalStats(degraded int64, oldestDegradedAgeSeconds, oldestReadyTaskAgeSeconds float64, instancesByQueue map[string]int64) {
	SetGauge(InstanceDegradedCurrent, float64(degraded))
	SetGauge(InstanceDegradedOldestAgeSeconds, oldestDegradedAgeSeconds)
	SetGauge(OldestReadyTaskAgeSeconds, oldestReadyTaskAgeSeconds)
	if InstancesByQueue != nil {
		instancesByQueueMu.Lock()
		defer instancesByQueueMu.Unlock()
		for queue := range instancesByQueueSeen {
			if _, still := instancesByQueue[queue]; !still {
				InstancesByQueue.DeleteLabelValues(queue)
				delete(instancesByQueueSeen, queue)
			}
		}
		for queue, n := range instancesByQueue {
			InstancesByQueue.WithLabelValues(queue).Set(float64(n))
			instancesByQueueSeen[queue] = struct{}{}
		}
	}
}

var (
	instancesByQueueMu   sync.Mutex
	instancesByQueueSeen = map[string]struct{}{}
)
