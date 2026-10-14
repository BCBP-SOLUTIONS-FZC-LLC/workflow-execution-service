package observability_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func okActivity(_ context.Context) error { return nil }

func failingActivity(_ context.Context) error { return errors.New("boom") }

func activitiesWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	if err := workflow.ExecuteActivity(ctx, okActivity).Get(ctx, nil); err != nil {
		return err
	}
	_ = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         nil,
	}), failingActivity).Get(ctx, nil)
	return nil
}

func panickingWorkflow(workflow.Context) error { panic("non-deterministic") }

func TestTemporalMetricsInterceptor_ObservesActivityDurationByOutcome(t *testing.T) {
	obstest.Init(t)
	const name = "workflow_execution_service_workflow_activity_duration_seconds"
	okBefore := obstest.Sum(t, name, map[string]string{"activity_name": "okActivity", "outcome": "success"})
	failBefore := obstest.Sum(t, name, map[string]string{"activity_name": "failingActivity", "outcome": "error"})

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(activitiesWorkflow)
	env.RegisterActivity(okActivity)
	env.RegisterActivity(failingActivity)
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()}})

	env.ExecuteWorkflow(activitiesWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, okBefore+1, obstest.Sum(t, name, map[string]string{"activity_name": "okActivity", "outcome": "success"}))
	require.GreaterOrEqual(t, obstest.Sum(t, name, map[string]string{"activity_name": "failingActivity", "outcome": "error"}), failBefore+1)
}

func TestTemporalMetricsInterceptor_CountsWorkflowPanics(t *testing.T) {
	obstest.Init(t)
	const name = "workflow_execution_service_workflow_replay_failures_total"
	before := obstest.Sum(t, name, map[string]string{"workflow_type": "panickingWorkflow"})

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(panickingWorkflow)
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()}})

	env.ExecuteWorkflow(panickingWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, before+1, obstest.Sum(t, name, map[string]string{"workflow_type": "panickingWorkflow"}))
}
