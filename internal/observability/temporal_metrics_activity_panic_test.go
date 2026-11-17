package observability_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func panickingActivity(context.Context) error { panic("activity blew up") }

func panickingActivityWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	_ = workflow.ExecuteActivity(ctx, panickingActivity).Get(ctx, nil)
	return nil
}

func TestTemporalMetricsInterceptor_AnActivityThatPanicsIsObservedAsAnError(t *testing.T) {
	obstest.Init(t)
	const name = "workflow_execution_service_workflow_activity_duration_seconds"
	labels := map[string]string{"activity_name": "panickingActivity", "outcome": "error"}
	before := obstest.Sum(t, name, labels)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(panickingActivityWorkflow)
	env.RegisterActivity(panickingActivity)
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()}})

	env.ExecuteWorkflow(panickingActivityWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, before+1, obstest.Sum(t, name, labels))
}
