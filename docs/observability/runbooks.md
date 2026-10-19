# Runbooks: workflow-execution-service alerts

Runbooks for every alert in [`deploy/monitoring/app-alerts.yml`](../../deploy/monitoring/app-alerts.yml) and the Helm [`PrometheusRule`](../../deploy/helm/templates/prometheusrule.yaml) (the same rules). The heading is the alert name, and the alert's `runbook` annotation (`workflow-execution-service/<slug>`) names the entry.

The service runs as an API (`job="workflow-execution-service-api"`), a Worker that polls the per-tenant Temporal queues (`job="workflow-execution-service-worker"`) and a connector worker. The LLD sections cited by the alerts are in the Execution Service LLD.

Common starting points:

- Pods: `kubectl get pods -l app.kubernetes.io/name=workflow-execution-service`, then `kubectl logs` of the failing pod; log lines carry `trace_id` and `request_id`.
- Error bodies are RFC 9457 problem documents with a stable `code`, `trace_id` and `request_id`.
- Readiness: `GET /readyz` answers 503 naming the first unreachable dependency among Postgres, Valkey and Temporal.
- Metric names are in [`metric-registry.md`](metric-registry.md).

## Availability

### ExecutionServiceAPIDown / ExecutionServiceAPIReplicasMissing

`runbook: workflow-execution-service/api-down`

**Meaning.** No API pod (or fewer than two) is scraped as healthy. Task claims, completions and instance starts fail.

**Triage.** `kubectl describe pod` for crash loops, probe failures and pending pods; check node pressure and the HPA. Startup refuses bad configuration and names the setting in the first log line. The schema is applied by the migrate job, not at boot. `/readyz` names the dependency that keeps a pod out of rotation.

**Mitigate.** Roll back the last deploy, or fix the named setting or dependency.

### ExecutionServiceWorkerDown / ExecutionServiceWorkerReplicasMissing

`runbook: workflow-execution-service/worker-down`

**Meaning.** No Worker pod (or fewer than two) is scraped as healthy. Running workflows make no progress and timers do not fire; they resume when a Worker polls again, because Temporal holds the state.

**Triage.** As for the API, per Worker pod; check the connection to the Temporal frontend and its namespace and certificates.

## HTTP errors and latency

### ExecutionServiceHighErrorRate / ExecutionServiceCriticalErrorRate

`runbook: workflow-execution-service/high-error-rate`

**Meaning.** More than 5% (20% for critical) of API requests return 5xx.

**Triage.**
1. Group by route: `sum by (route) (rate(platform_http_requests_total{job="workflow-execution-service-api",status_class="5xx"}[5m]))`.
2. Check Postgres, PgBouncer, Valkey and the Temporal frontend, and whether a deploy started at the same time.
3. A failure on the two routes the Definition Service calls is covered by `ExecutionServiceDefinitionCallsHighErrorRate`; a failing plan read shows as `UpstreamDependencyErrors` with `dependency=definition_service`.

**Mitigate.** Roll back the last deploy; restore the failing dependency.

### ExecutionServiceHighLatency

`runbook: workflow-execution-service/high-latency`

**Meaning.** API p99 request duration is above 2 s.

**Triage.** Find the slow route with `histogram_quantile(0.99, sum by (le, route) (rate(platform_http_request_duration_seconds_bucket{job="workflow-execution-service-api"}[5m])))`. Slow signals point at the Temporal frontend or Postgres; a slow start also waits on the compiled-plan read.

### ExecutionServiceDefinitionCallsHighErrorRate

`runbook: workflow-execution-service/definition-calls`

**Meaning.** The Definition Service calls two routes of this API, `active-instances` and `pause-tasks`, and the named route answers 5xx to more than 5% of those calls. The alert measures this service's own inbound routes, not its calls to the Definition Service.

**Triage.** The failing route is in the alert. The Definition Service's publish and departure handling wait on these answers, so check Postgres and the Temporal frontend for this API and recent deploys. For the reverse direction, a plan read that fails, see `UpstreamDependencyErrors` and the Definition Service's `compiled-plan` runbook; a request that cannot get its plan answers 503 with the upstream-unavailable code and does nothing.

### ExecutionServiceHTTPPanic

`runbook: workflow-execution-service/panic`

**Meaning.** A handler panicked and the middleware recovered it. This should never happen.

**Triage.** The stack trace is in the logs next to the request's `trace_id`; open a defect with the request shape.

## Database

### ExecutionServiceAPIDBErrorRate / ExecutionServiceWorkerDBErrorRate

`runbook: workflow-execution-service/db-errors`

**Meaning.** More than 5% of the API's (or the Worker's) queries error.

**Triage.** Check Postgres and PgBouncer (pool exhaustion, failover, a long lock).

## Instances

### InstanceDegraded

`runbook: workflow-execution-service/instance-degraded`

**Meaning.** A parallel-branch failure parked an instance in DEGRADED; it waits for an administrator.

**Triage.** `GET /api/v1/instances/{id}` and its events show the failed branch. Resolve with `POST /api/v1/instances/{id}/force-forward` (continue past the failed node) or `/force-back` (return to an earlier stage). A pause set before or during DEGRADED stays in force until the instance is resumed.

### DegradedBacklogStale

`runbook: workflow-execution-service/instance-degraded`

**Meaning.** An instance has been DEGRADED for over a day, and the entry alert fires only once.

**Triage.** As for `InstanceDegraded`; find the oldest with the instance list filtered by status.

### WorkflowReplayFailure

`runbook: workflow-execution-service/replay-failure`

**Meaning.** Temporal replay of a workflow hit non-determinism, which is fatal to that workflow execution.

**Triage.** The workflow type is in the alert. The cause is almost always a deploy that changed workflow code without a `GetVersion` change ID. Roll back to the previous image so the open executions replay again, then ship the change behind a change ID with a replay fixture (`test/workflow/replay`).

## Outbox and events

### OutboxDeadLettered

`runbook: workflow-execution-service/outbox`

**Meaning.** An outbound event failed all its publish retries and was dead-lettered.

**Triage.** The event type is in the alert. Check the publisher's target (topic, credentials, schema registry) and fix it; the dead-lettered event is replayed through the outbox runner's DLQ replay (LLD §6.6).

### OutboxRelayWedged

`runbook: workflow-execution-service/outbox`

**Meaning.** The oldest pending outbox row is more than 10 minutes old, so the relay is not publishing. A relay that has stopped produces no attempts and no dead letters.

**Triage.** The relay runs inside the API process, so look at the API pods' logs for relay errors or for no relay lines at all (`outbox relay starting` is logged at boot), and for a long lock or a stuck transaction on the outbox table. The Worker only writes outbox rows.

### InboundEventIngestErrors

`runbook: workflow-execution-service/event-ingest`

**Meaning.** Inbound events are failing at the internal event routes.

**Triage.** The `result="error"` series names the event type. The shared consumer redelivers a failed message until its receive threshold is spent and then dead-letters it; see the consumer's runbook for the queue side.

### SharedWorkflowEventsConsumerOutage

`runbook: workflow-execution-service/consumer-outage`

**Meaning.** No `DelegationStarted` or `TenantStateChanged` event arrived in 15 minutes. These types are live, so silence points at the consumer, not at idle tenants.

**Triage.** Check the consumer's health and queue depth, and its forward errors to this service.

### TenantPurgeFailing

`runbook: workflow-execution-service/tenant-purge`

**Meaning.** An offboarded tenant's rows could not be fully deleted after the retention period. The sweep retries on its next run.

**Triage.** The `tenant purge failed` log line names the tenant and the error. Nothing needs doing once a later run succeeds.

## Queues and workers

### WorkerQueuePollingStalled

`runbook: workflow-execution-service/queue-polling`

**Meaning.** The named tenant queue has not been polled in 5 minutes, although the Worker is up.

**Triage.** Look in the Worker logs for `failed to start worker for queue` for that queue (a worker that cannot start for one queue only logs). The topology poll retries every queue without a registered worker on each interval, so a persistent failure names its own cause in that line; a restart is only needed when the cause is fixed and the next poll has not yet passed.

### InstancesOnUnpolledQueue

`runbook: workflow-execution-service/queue-polling`

**Meaning.** The named queue has running instances and no Worker polling it, so they wait with no task and no other signal.

**Triage.** As for `WorkerQueuePollingStalled`; the instances resume when a Worker polls the queue.

### UpstreamDependencyErrors

`runbook: workflow-execution-service/upstream-dependency`

**Meaning.** Calls to the named dependency (`definition_service` or `iam`) fail at a sustained rate, apart from the API's own 5xx rate.

**Triage.** Check that dependency's health. A plan-read failure fails the request that needed the plan with a 503; an IAM failure does the same for the operation that needed the eligibility answer.

### CompiledPlanCacheMissRatioHigh

`runbook: workflow-execution-service/plan-cache`

**Meaning.** More than 10% of workflow-cache lookups miss.

**Triage.** A Worker restart or a large rollout empties the cache. If it persists, check the sticky cache size setting and the pod memory limits.

## Service levels

### TaskSignalSLOBreach / InstantiationSLOBreach / DelegationRerouteSLOBreach

`runbook: workflow-execution-service/slo-breach`

**Meaning.** p99 of task signals (300 ms), instance start (1 s) or delegation reroute (4 s) is above its objective.

**Triage.** Find which step is slow from the traces of a slow request: the Temporal frontend, Postgres, the plan read, or (for a reroute) the batch of eligibility checks against IAM. Compare with the matching dependency alert.

## Tenant isolation

### RLSViolation

`runbook: workflow-execution-service/rls`

**Meaning.** A connection queried a tenant table without a valid tenant setting (`missing_or_invalid_guc`), or row-level security blocked access to another tenant's rows (`cross_tenant_access`).

**Triage.** The sampled rows are in the `rls_violation_log` table (the offending query text and the violation type); the exporter only counts them and prunes them after the retention period, so read the rows before they expire. A missing setting is a route or a raw session without the tenant middleware. A cross-tenant block is a security event: find the query, and check for a service bug that passes the wrong tenant and for probing.
