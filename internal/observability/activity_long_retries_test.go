package observability_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const activityLongRetriesName = "workflow_execution_service_activity_long_retries_total"

func succeedsOnAttempt(succeeds int) func(context.Context) error {
	return func(ctx context.Context) error {
		if int(activity.GetInfo(ctx).Attempt) < succeeds {
			return errors.New("not yet")
		}
		return nil
	}
}

func unlimitedRetryWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    20,
		},
	})
	return workflow.ExecuteActivity(ctx, "retried").Get(ctx, nil)
}

func TestTemporalMetricsInterceptor_CountsEveryActivityAttemptBeyondTheTenth(t *testing.T) {
	cases := []struct {
		succeedsOnAttempt int
		want              float64
	}{
		{succeedsOnAttempt: 1, want: 0},
		{succeedsOnAttempt: 10, want: 0},
		{succeedsOnAttempt: 11, want: 1},
		{succeedsOnAttempt: 13, want: 3},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("succeeds on attempt %d", tc.succeedsOnAttempt), func(t *testing.T) {
			obstest.Init(t)
			before := obstest.Sum(t, activityLongRetriesName, nil)

			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetWorkflowRunTimeout(time.Hour)
			env.RegisterWorkflow(unlimitedRetryWorkflow)
			env.RegisterActivityWithOptions(succeedsOnAttempt(tc.succeedsOnAttempt), activity.RegisterOptions{Name: "retried"})
			env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()}})

			env.ExecuteWorkflow(unlimitedRetryWorkflow)

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, before+tc.want, obstest.Sum(t, activityLongRetriesName, nil))
		})
	}
}
