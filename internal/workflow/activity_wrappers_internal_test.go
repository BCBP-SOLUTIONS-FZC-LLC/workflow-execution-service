package workflow

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// wflWrapperCase names one activity wrapper, the activity it must call and the
// call that drives it.
type wflWrapperCase struct {
	activity string
	call     func(ctx wf.Context) error
}

func wflWrapperCases() []wflWrapperCase {
	return []wflWrapperCase{
		{"GetCompiledPlanActivity", func(ctx wf.Context) error {
			_, err := getCompiledPlan(ctx, port.GetCompiledPlanInput{TenantID: "t-1", VersionID: "v-1"})
			return err
		}},
		{"CreateTaskActivity", func(ctx wf.Context) error {
			_, err := createTask(ctx, port.CreateTaskInput{TenantID: "t-1", InstanceID: "i-1"})
			return err
		}},
		{"UpdateInstanceNodesActivity", func(ctx wf.Context) error {
			return updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{TenantID: "t-1", InstanceID: "i-1"})
		}},
		{"CompleteAssignmentActivity", func(ctx wf.Context) error {
			_, err := completeAssignment(ctx, port.CompleteAssignmentInput{TenantID: "t-1", TaskID: "task-1"})
			return err
		}},
		{"UpdateInstanceStatusActivity", func(ctx wf.Context) error {
			return updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{TenantID: "t-1", InstanceID: "i-1"})
		}},
		{"DeferTaskActivity", func(ctx wf.Context) error {
			return deferTask(ctx, port.DeferTaskInput{TenantID: "t-1", TaskID: "task-1"})
		}},
		{"RecordForceRouteActivity", func(ctx wf.Context) error {
			return recordForceRoute(ctx, port.RecordForceRouteInput{TenantID: "t-1", InstanceID: "i-1"})
		}},
		{"RecordSLAWarningActivity", func(ctx wf.Context) error {
			return recordSLAWarning(ctx, port.RecordSLAWarningInput{TenantID: "t-1", TaskID: "task-1"})
		}},
		{"RecordSLABreachActivity", func(ctx wf.Context) error {
			return recordSLABreach(ctx, port.RecordSLABreachInput{TenantID: "t-1", TaskID: "task-1"})
		}},
		{"PauseInstanceActivity", func(ctx wf.Context) error {
			return pauseInstance(ctx, port.PauseInstanceInput{TenantID: "t-1", InstanceID: "i-1"})
		}},
		{"ResumeInstanceActivity", func(ctx wf.Context) error {
			return resumeInstance(ctx, port.ResumeInstanceInput{TenantID: "t-1", InstanceID: "i-1"})
		}},
		{"CancelInstanceActivity", func(ctx wf.Context) error {
			return cancelInstance(ctx, port.CancelInstanceInput{TenantID: "t-1", InstanceID: "i-1"})
		}},
		{"ReassignAssignmentActivity", func(ctx wf.Context) error {
			return reassignAssignment(ctx, port.ReassignAssignmentInput{TenantID: "t-1", TaskID: "task-1"})
		}},
		{"UpdateTaskStatusActivity", func(ctx wf.Context) error {
			return updateTaskStatus(ctx, port.UpdateTaskStatusInput{TenantID: "t-1", TaskID: "task-1"})
		}},
	}
}

// Each of the fourteen activity wrappers (the compiled-plan read and the
// thirteen database writes) hands its failure back as
// "workflow: <activity name>: <cause>" after calling exactly the activity of
// that name once.
func TestActivityWrappers_FailureIsReturnedNamingTheActivity(t *testing.T) {
	cases := wflWrapperCases()
	require.Len(t, cases, 14)
	for _, tc := range cases {
		t.Run(tc.activity, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var mu sync.Mutex
			called := map[string]int{}
			for _, c := range cases {
				name := c.activity
				env.RegisterActivityWithOptions(func(_ context.Context, _ any) error {
					mu.Lock()
					called[name]++
					mu.Unlock()
					return temporal.NewNonRetryableApplicationError("backing store unavailable", "ValidationError", nil)
				}, activity.RegisterOptions{Name: name})
			}

			env.ExecuteWorkflow(func(ctx wf.Context) (string, error) {
				err := tc.call(ctx)
				if err == nil {
					return "", nil
				}
				return err.Error(), nil
			})

			require.NoError(t, env.GetWorkflowError())
			var msg string
			require.NoError(t, env.GetWorkflowResult(&msg))
			assert.True(t, strings.HasPrefix(msg, "workflow: "+tc.activity+": "), "error text %q", msg)
			assert.Contains(t, msg, "backing store unavailable")
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, map[string]int{tc.activity: 1}, called)
		})
	}
}
