package workflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w23aOptionsRecorder is a worker interceptor that records the activity options
// each outgoing activity call carried, by activity name.
type w23aOptionsRecorder struct {
	interceptor.WorkerInterceptorBase
	mu      sync.Mutex
	options map[string]wf.ActivityOptions
}

func (r *w23aOptionsRecorder) InterceptWorkflow(_ wf.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &w23aInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{}, next: next, rec: r}
}

type w23aInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	next interceptor.WorkflowInboundInterceptor
	rec  *w23aOptionsRecorder
}

func (i *w23aInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.next.Init(&w23aOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, rec: i.rec})
}

func (i *w23aInbound) ExecuteWorkflow(ctx wf.Context, in *interceptor.ExecuteWorkflowInput) (interface{}, error) {
	return i.next.ExecuteWorkflow(ctx, in)
}

type w23aOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	rec *w23aOptionsRecorder
}

func (o *w23aOutbound) ExecuteActivity(ctx wf.Context, activityType string, args ...interface{}) wf.Future {
	o.rec.mu.Lock()
	o.rec.options[activityType] = wf.GetActivityOptions(ctx)
	o.rec.mu.Unlock()
	return o.WorkflowOutboundInterceptorBase.ExecuteActivity(ctx, activityType, args...)
}

// w23aWrapperCalls drives every activity wrapper once, in the order of the port
// activity names.
func w23aWrapperCalls() map[string]func(ctx wf.Context) error {
	return map[string]func(ctx wf.Context) error{
		port.ActivityGetCompiledPlan: func(ctx wf.Context) error {
			_, err := getCompiledPlan(ctx, port.GetCompiledPlanInput{TenantID: "t-1", VersionID: "v-1"})
			return err
		},
		port.ActivityCreateTask: func(ctx wf.Context) error {
			_, err := createTask(ctx, port.CreateTaskInput{TenantID: "t-1", InstanceID: "i-1"})
			return err
		},
		port.ActivityUpdateInstanceNodes: func(ctx wf.Context) error {
			return updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{TenantID: "t-1", InstanceID: "i-1"})
		},
		port.ActivityCompleteAssignment: func(ctx wf.Context) error {
			_, err := completeAssignment(ctx, port.CompleteAssignmentInput{TenantID: "t-1", TaskID: "task-1"})
			return err
		},
		port.ActivityUpdateInstanceStatus: func(ctx wf.Context) error {
			return updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{TenantID: "t-1", InstanceID: "i-1"})
		},
		port.ActivityDeferTask: func(ctx wf.Context) error {
			return deferTask(ctx, port.DeferTaskInput{TenantID: "t-1", TaskID: "task-1"})
		},
		port.ActivityRecordForceRoute: func(ctx wf.Context) error {
			return recordForceRoute(ctx, port.RecordForceRouteInput{TenantID: "t-1", InstanceID: "i-1"})
		},
		port.ActivityRecordSLAWarning: func(ctx wf.Context) error {
			return recordSLAWarning(ctx, port.RecordSLAWarningInput{TenantID: "t-1", TaskID: "task-1"})
		},
		port.ActivityRecordSLABreach: func(ctx wf.Context) error {
			return recordSLABreach(ctx, port.RecordSLABreachInput{TenantID: "t-1", TaskID: "task-1"})
		},
		port.ActivityPauseInstance: func(ctx wf.Context) error {
			return pauseInstance(ctx, port.PauseInstanceInput{TenantID: "t-1", InstanceID: "i-1"})
		},
		port.ActivityResumeInstance: func(ctx wf.Context) error {
			return resumeInstance(ctx, port.ResumeInstanceInput{TenantID: "t-1", InstanceID: "i-1"})
		},
		port.ActivityCancelInstance: func(ctx wf.Context) error {
			return cancelInstance(ctx, port.CancelInstanceInput{TenantID: "t-1", InstanceID: "i-1"})
		},
		port.ActivityReassignAssignment: func(ctx wf.Context) error {
			return reassignAssignment(ctx, port.ReassignAssignmentInput{TenantID: "t-1", TaskID: "task-1"})
		},
		port.ActivityUpdateTaskStatus: func(ctx wf.Context) error {
			return updateTaskStatus(ctx, port.UpdateTaskStatusInput{TenantID: "t-1", TaskID: "task-1"})
		},
	}
}

// w23aRecordedOptions runs the named wrapper against a succeeding activity and
// returns the options its call carried.
func w23aRecordedOptions(t *testing.T, name string) wf.ActivityOptions {
	t.Helper()
	rec := &w23aOptionsRecorder{options: map[string]wf.ActivityOptions{}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{rec}})
	var calls atomic.Int32
	for activityName := range w23aWrapperCalls() {
		env.RegisterActivityWithOptions(func(_ context.Context, _ any) error {
			calls.Add(1)
			return nil
		}, activity.RegisterOptions{Name: activityName})
	}
	call := w23aWrapperCalls()[name]
	env.ExecuteWorkflow(func(ctx wf.Context) error { return call(ctx) })
	require.NoError(t, env.GetWorkflowError())
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.options, 1, "the wrapper must call exactly the activity %s", name)
	opts, ok := rec.options[name]
	require.True(t, ok, "no call of %s was recorded", name)
	return opts
}

// Each of the thirteen database-write wrappers calls its activity with a 30s
// StartToClose, a retry of 1s doubling up to 60s, no attempt limit, and
// ValidationError, NotFoundError and VersionConflict as the types never retried.
func TestDBWriteWrappers_CarryTheDatabaseWriteOptions(t *testing.T) {
	var names []string
	for name := range w23aWrapperCalls() {
		if name != port.ActivityGetCompiledPlan {
			names = append(names, name)
		}
	}
	require.Len(t, names, 13)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := w23aRecordedOptions(t, name)
			assert.Equal(t, 30*time.Second, opts.StartToCloseTimeout)
			require.NotNil(t, opts.RetryPolicy)
			assert.Equal(t, time.Second, opts.RetryPolicy.InitialInterval)
			assert.InDelta(t, 2.0, opts.RetryPolicy.BackoffCoefficient, 0)
			assert.Equal(t, 60*time.Second, opts.RetryPolicy.MaximumInterval)
			assert.EqualValues(t, 0, opts.RetryPolicy.MaximumAttempts, "unlimited attempts")
			assert.Equal(t, []string{"ValidationError", "NotFoundError", "VersionConflict"}, opts.RetryPolicy.NonRetryableErrorTypes)
		})
	}
}

// The compiled-plan read calls its activity with the external-call options: a
// 10s StartToClose, a retry of 500ms doubling up to 30s, no attempt limit, and
// only DefinitionServiceClientError never retried.
func TestGetCompiledPlan_CarriesTheExternalCallOptions(t *testing.T) {
	opts := w23aRecordedOptions(t, port.ActivityGetCompiledPlan)
	assert.Equal(t, 10*time.Second, opts.StartToCloseTimeout)
	require.NotNil(t, opts.RetryPolicy)
	assert.Equal(t, 500*time.Millisecond, opts.RetryPolicy.InitialInterval)
	assert.InDelta(t, 2.0, opts.RetryPolicy.BackoffCoefficient, 0)
	assert.Equal(t, 30*time.Second, opts.RetryPolicy.MaximumInterval)
	assert.EqualValues(t, 0, opts.RetryPolicy.MaximumAttempts, "unlimited attempts")
	assert.Equal(t, []string{"DefinitionServiceClientError"}, opts.RetryPolicy.NonRetryableErrorTypes)
}

// The plan read retries the error types the database writes refuse to retry:
// only DefinitionServiceClientError is final here.
func TestGetCompiledPlan_RetriesAValidationErrorUntilItSucceeds(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var mu sync.Mutex
	var inputs []port.GetCompiledPlanInput
	env.RegisterActivityWithOptions(func(_ context.Context, in port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
		mu.Lock()
		defer mu.Unlock()
		inputs = append(inputs, in)
		if len(inputs) <= 2 {
			return port.GetCompiledPlanOutput{}, temporal.NewApplicationError("looks invalid", "ValidationError")
		}
		return port.GetCompiledPlanOutput{}, nil
	}, activity.RegisterOptions{Name: port.ActivityGetCompiledPlan})

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		_, err := getCompiledPlan(ctx, port.GetCompiledPlanInput{TenantID: "tenant-9", VersionID: "version-9"})
		return err
	})

	require.NoError(t, env.GetWorkflowError())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, inputs, 3, "two refused reads and the one that succeeded")
	for _, in := range inputs {
		assert.Equal(t, port.GetCompiledPlanInput{TenantID: "tenant-9", VersionID: "version-9"}, in)
	}
}
