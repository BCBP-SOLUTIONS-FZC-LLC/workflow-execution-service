package observability

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

type temporalMetricsInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func NewTemporalMetricsInterceptor() interceptor.WorkerInterceptor {
	return &temporalMetricsInterceptor{}
}

func (i *temporalMetricsInterceptor) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityMetrics{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}}
}

func (i *temporalMetricsInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowMetrics{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type activityMetrics struct {
	interceptor.ActivityInboundInterceptorBase
}

func (a *activityMetrics) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (result interface{}, err error) {
	start := time.Now()
	name := activity.GetInfo(ctx).ActivityType.Name
	outcome := "error"
	defer func() {
		ObserveHistogramVec(WorkflowActivityDurationSeconds, time.Since(start).Seconds(), name, outcome)
	}()
	result, err = a.Next.ExecuteActivity(ctx, in)
	if err == nil {
		outcome = "success"
	}
	return result, err //nolint:wrapcheck // the SDK classifies activity errors by type, so they pass through unwrapped
}

type workflowMetrics struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (w *workflowMetrics) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (result interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			IncCounterVec(WorkflowReplayFailuresTotal, workflow.GetInfo(ctx).WorkflowType.Name)
			panic(r)
		}
	}()
	return w.Next.ExecuteWorkflow(ctx, in) //nolint:wrapcheck // the SDK classifies workflow errors by type, so they pass through unwrapped
}
