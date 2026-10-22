package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

var dynamicQueueWorkerOptions = worker.Options{
	MaxConcurrentActivityExecutionSize:     200,
	MaxConcurrentWorkflowTaskExecutionSize: 200,
	WorkerStopTimeout:                      25 * time.Second,
	Interceptors:                           []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()},
}

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

func registerAll(w worker.Worker, deps *outboundtemporal.Deps) {
	w.RegisterWorkflowWithOptions(wfengine.Execute, sdkworkflow.RegisterOptions{Name: port.WorkflowTypeExecute})
	registerActivities(w, deps)
}

func startWorkerForQueue(sdk client.Client, deps *outboundtemporal.Deps, queueName string, opts worker.Options) (worker.Worker, error) {
	w := worker.New(sdk, queueName, opts)
	registerAll(w, deps)
	if err := w.Start(); err != nil {
		return nil, fmt.Errorf("start worker for queue %q: %w", queueName, err)
	}
	return w, nil
}

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
		if q.QueueName == defaultQueue {
			continue
		}
		if registry.has(q.QueueName) {
			observability.WorkerQueueLastPollTimestamp.WithLabelValues(q.QueueName).SetToCurrentTime()
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
