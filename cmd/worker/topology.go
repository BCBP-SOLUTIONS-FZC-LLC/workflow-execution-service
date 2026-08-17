package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// dynamicQueueWorkerOptions caps per-tenant queue concurrency below the
// default queue's own defaults so one busy tenant can't starve pool/DB
// capacity shared with the default queue. Placeholder values pending real
// sizing (LLD §3.2 flags the whole capacity model as provisional).
var dynamicQueueWorkerOptions = worker.Options{
	MaxConcurrentActivityExecutionSize:     200,
	MaxConcurrentWorkflowTaskExecutionSize: 200,
	WorkerStopTimeout:                      25 * time.Second,
}

// workerRegistry tracks every worker.Worker this process has started, keyed
// by task queue name, so the topology poller never double-starts one and
// shutdown can stop them all.
type workerRegistry struct {
	mu      sync.Mutex
	workers map[string]worker.Worker
}

func newWorkerRegistry() *workerRegistry {
	return &workerRegistry{workers: make(map[string]worker.Worker)}
}

func (r *workerRegistry) add(queueName string, w worker.Worker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[queueName] = w
}

func (r *workerRegistry) has(queueName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.workers[queueName]
	return ok
}

func (r *workerRegistry) all() []worker.Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]worker.Worker, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, w)
	}
	return out
}

// registerAll registers the Execute workflow and every Activity in deps
// against w. Must stay in lockstep with internal/core/port/activities.go —
// checked by test/workflow's fake-activity registration mirroring the same
// name list.
func registerAll(w worker.Worker, deps *outboundtemporal.Deps) {
	w.RegisterWorkflowWithOptions(wfengine.Execute, sdkworkflow.RegisterOptions{Name: port.WorkflowTypeExecute})
	registerActivities(w, deps)
}

// startWorkerForQueue constructs, registers, and starts (non-blocking) a
// worker.Worker on queueName. Multiple worker.Worker instances safely share
// one client.Client (Stop never touches the client, only its own pollers).
func startWorkerForQueue(sdk client.Client, deps *outboundtemporal.Deps, queueName string, opts worker.Options) (worker.Worker, error) {
	w := worker.New(sdk, queueName, opts)
	registerAll(w, deps)
	if err := w.Start(); err != nil {
		return nil, fmt.Errorf("start worker for queue %q: %w", queueName, err)
	}
	return w, nil
}

// pollQueueTopology polls queues.ListActive on interval and starts a new
// worker.Worker for every queue not yet in registry — additive only. A
// queue whose row later disappears keeps its already-started worker running
// (LLD §3.2: a running workflow execution is permanently bound to the task
// queue it started on; stopping an idle worker for a deregistered queue is a
// possible future optimization, not a correctness requirement here). Every
// registered queue's own tenant identity is irrelevant to the loop itself —
// only the queue name is used to start/dedupe workers.
func pollQueueTopology(
	ctx context.Context,
	sdk client.Client,
	deps *outboundtemporal.Deps,
	queues port.ActiveTaskQueueRepository,
	defaultQueue string,
	interval time.Duration,
	registry *workerRegistry,
	log port.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pollQueueTopologyOnce(ctx, sdk, deps, queues, defaultQueue, registry, log)
		}
	}
}

func pollQueueTopologyOnce(
	ctx context.Context,
	sdk client.Client,
	deps *outboundtemporal.Deps,
	queues port.ActiveTaskQueueRepository,
	defaultQueue string,
	registry *workerRegistry,
	log port.Logger,
) {
	listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	active, err := queues.ListActive(listCtx)
	if err != nil {
		log.Warn("queue topology poll failed", map[string]any{"error": err.Error()})
		return
	}

	observability.WorkerActiveQueues.Set(float64(len(active)))
	observability.WorkerQueueLastPollTimestamp.WithLabelValues(defaultQueue).SetToCurrentTime()

	for _, q := range active {
		if q.QueueName == defaultQueue || registry.has(q.QueueName) {
			continue
		}
		w, err := startWorkerForQueue(sdk, deps, q.QueueName, dynamicQueueWorkerOptions)
		if err != nil {
			log.Error("failed to start worker for queue", map[string]any{"queue": q.QueueName, "error": err.Error()})
			continue
		}
		registry.add(q.QueueName, w)
		observability.WorkerQueueLastPollTimestamp.WithLabelValues(q.QueueName).SetToCurrentTime()
		log.Info("started worker for dynamic queue", map[string]any{"queue": q.QueueName, "tenant_id": q.TenantID.String()})
	}
}

// registerActivities registers every Activity constant in
// internal/core/port/activities.go against its implementation in deps —
// the two must stay in lockstep, checked by test/workflow's fake-activity
// registration mirroring the same name list.
func registerActivities(w worker.Worker, deps *outboundtemporal.Deps) {
	w.RegisterActivityWithOptions(deps.GetCompiledPlan, activity.RegisterOptions{Name: port.ActivityGetCompiledPlan})
	w.RegisterActivityWithOptions(deps.CreateTask, activity.RegisterOptions{Name: port.ActivityCreateTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceNodes, activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes})
	w.RegisterActivityWithOptions(deps.ClaimAssignment, activity.RegisterOptions{Name: port.ActivityClaimAssignment})
	w.RegisterActivityWithOptions(deps.CompleteAssignment, activity.RegisterOptions{Name: port.ActivityCompleteAssignment})
	w.RegisterActivityWithOptions(deps.DeferTask, activity.RegisterOptions{Name: port.ActivityDeferTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceStatus, activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus})
	w.RegisterActivityWithOptions(deps.RecordForceRoute, activity.RegisterOptions{Name: port.ActivityRecordForceRoute})
	w.RegisterActivityWithOptions(deps.RecordSLAWarning, activity.RegisterOptions{Name: port.ActivityRecordSLAWarning})
	w.RegisterActivityWithOptions(deps.RecordSLABreach, activity.RegisterOptions{Name: port.ActivityRecordSLABreach})
	w.RegisterActivityWithOptions(deps.PauseInstance, activity.RegisterOptions{Name: port.ActivityPauseInstance})
	w.RegisterActivityWithOptions(deps.ResumeInstance, activity.RegisterOptions{Name: port.ActivityResumeInstance})
	w.RegisterActivityWithOptions(deps.CancelInstance, activity.RegisterOptions{Name: port.ActivityCancelInstance})
	w.RegisterActivityWithOptions(deps.ReassignAssignment, activity.RegisterOptions{Name: port.ActivityReassignAssignment})
	w.RegisterActivityWithOptions(deps.UpdateTaskStatus, activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus})
}
