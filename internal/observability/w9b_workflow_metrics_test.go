package observability_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var errW9bWorkflowFailed = errors.New("workflow failed on purpose")

func w9bFailingWorkflow(workflow.Context) error { return errW9bWorkflowFailed }

func w9bCompletingWorkflow(workflow.Context) error { return nil }

func TestTemporalMetricsInterceptor_WorkflowThatReturnsOrFails_DoesNotCountAReplayFailure(t *testing.T) {
	cases := []struct {
		name      string
		workflow  func(workflow.Context) error
		wantError bool
	}{
		{name: "workflow returns an error", workflow: w9bFailingWorkflow, wantError: true},
		{name: "workflow completes normally", workflow: w9bCompletingWorkflow, wantError: false},
	}
	const family = "workflow_execution_service_workflow_replay_failures_total"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			before := obstest.Sum(t, family, nil)

			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(tc.workflow)
			env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{observability.NewTemporalMetricsInterceptor()}})

			env.ExecuteWorkflow(tc.workflow)

			require.True(t, env.IsWorkflowCompleted())
			if tc.wantError {
				require.Error(t, env.GetWorkflowError(), "the workflow's own error must reach the caller")
			} else {
				require.NoError(t, env.GetWorkflowError())
			}
			assert.Equal(t, before, obstest.Sum(t, family, nil), "no workflow type's counter may move")
		})
	}
}
