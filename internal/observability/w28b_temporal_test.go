package observability_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

// w28bActivityTap records the error each attempt returns at the point where it sits in the chain.
type w28bActivityTap struct {
	interceptor.WorkerInterceptorBase
	mu      sync.Mutex
	results []error
}

func (tap *w28bActivityTap) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &w28bActivityTapInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, tap: tap}
}

type w28bActivityTapInbound struct {
	interceptor.ActivityInboundInterceptorBase
	tap *w28bActivityTap
}

func (in *w28bActivityTapInbound) ExecuteActivity(ctx context.Context, input *interceptor.ExecuteActivityInput) (interface{}, error) {
	result, err := in.Next.ExecuteActivity(ctx, input)
	in.tap.mu.Lock()
	defer in.tap.mu.Unlock()
	in.tap.results = append(in.tap.results, err)
	return result, err
}

func TestTemporalMetricsInterceptor_FailingActivity_ReturnsTheSameErrorValueAndCountsEveryAttempt(t *testing.T) {
	cases := []struct {
		name         string
		activityErr  error
		wantAttempts int
		wantNonRetry bool
	}{
		{
			name:         "non-retryable application error runs once and stays non-retryable",
			activityErr:  temporal.NewNonRetryableApplicationError("w28b stop here", "W28bStop", nil),
			wantAttempts: 1,
			wantNonRetry: true,
		},
		{
			name:         "plain error is retried and every attempt is counted",
			activityErr:  errors.New("w28b transient"),
			wantAttempts: 3,
			wantNonRetry: false,
		},
	}
	const family = "workflow_execution_service_workflow_activity_duration_seconds"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			labels := map[string]string{"activity_name": "w28bFailingActivity", "outcome": "error"}
			before := obstest.Sum(t, family, labels)
			attempts := 0
			outer, inner := &w28bActivityTap{}, &w28bActivityTap{}

			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterActivityWithOptions(func(context.Context) error {
				attempts++
				return tc.activityErr
			}, activity.RegisterOptions{Name: "w28bFailingActivity"})
			env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: 10 * time.Second,
					RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3, InitialInterval: time.Second},
				})
				return workflow.ExecuteActivity(ctx, "w28bFailingActivity").Get(ctx, nil)
			}, workflow.RegisterOptions{Name: "w28bFailingActivityWorkflow"})
			env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{outer, observability.NewTemporalMetricsInterceptor(), inner}})

			env.ExecuteWorkflow("w28bFailingActivityWorkflow")

			require.True(t, env.IsWorkflowCompleted())
			assert.Equal(t, tc.wantAttempts, attempts)
			assert.Equal(t, before+float64(tc.wantAttempts), obstest.Sum(t, family, labels), "one outcome=error sample per attempt")

			require.Len(t, inner.results, tc.wantAttempts)
			require.Len(t, outer.results, tc.wantAttempts)
			for i := range inner.results {
				require.Error(t, inner.results[i])
				assert.Same(t, inner.results[i], outer.results[i], "attempt %d: the interceptor must return the very error it received", i)
			}

			var activityErr *temporal.ActivityError
			require.ErrorAs(t, env.GetWorkflowError(), &activityErr)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, activityErr, &appErr)
			assert.Equal(t, tc.wantNonRetry, appErr.NonRetryable())
			if tc.wantNonRetry {
				assert.Equal(t, "W28bStop", appErr.Type())
			}
		})
	}
}

// w28bWorkflowPanicTap recovers a panic leaving the interceptor chain, notes the value and re-panics it.
type w28bWorkflowPanicTap struct {
	interceptor.WorkerInterceptorBase
	mu        sync.Mutex
	recovered []any
}

func (tap *w28bWorkflowPanicTap) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &w28bWorkflowPanicTapInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, tap: tap}
}

type w28bWorkflowPanicTapInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	tap *w28bWorkflowPanicTap
}

func (in *w28bWorkflowPanicTapInbound) ExecuteWorkflow(ctx workflow.Context, input *interceptor.ExecuteWorkflowInput) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			in.tap.mu.Lock()
			in.tap.recovered = append(in.tap.recovered, r)
			in.tap.mu.Unlock()
			panic(r)
		}
	}()
	return in.Next.ExecuteWorkflow(ctx, input)
}

type w28bPanicValue struct{ detail string }

func TestTemporalMetricsInterceptor_PanickingWorkflow_CountsOnceAndPanicsWithTheSameValue(t *testing.T) {
	obstest.Init(t)
	const family = "workflow_execution_service_workflow_replay_failures_total"
	labels := map[string]string{"workflow_type": "w28bPanickingWorkflow"}
	before := obstest.Sum(t, family, labels)
	otherBefore := obstest.Sum(t, family, nil) - before
	panicked := w28bPanicValue{detail: "w28b original panic value"}
	tap := &w28bWorkflowPanicTap{}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(func(workflow.Context) error { panic(panicked) }, workflow.RegisterOptions{Name: "w28bPanickingWorkflow"})
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{tap, observability.NewTemporalMetricsInterceptor()}})

	env.ExecuteWorkflow("w28bPanickingWorkflow")

	require.True(t, env.IsWorkflowCompleted())
	assert.Equal(t, before+1, obstest.Sum(t, family, labels))
	assert.Equal(t, otherBefore, obstest.Sum(t, family, nil)-(before+1), "no other workflow type moves")
	require.Len(t, tap.recovered, 1)
	assert.Equal(t, panicked, tap.recovered[0], "the panic continues with the value the workflow raised")
	var panicErr *temporal.PanicError
	require.ErrorAs(t, env.GetWorkflowError(), &panicErr)
	assert.Contains(t, panicErr.Error(), "w28b original panic value")
}

// w28bUpsertTap notes every search-attribute upsert the workflow sends and passes it on.
type w28bUpsertTap struct {
	interceptor.WorkerInterceptorBase
	calls [][]temporal.SearchAttributeUpdate
}

func (tap *w28bUpsertTap) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &w28bUpsertTapInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, tap: tap}
}

type w28bUpsertTapInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	tap *w28bUpsertTap
}

func (in *w28bUpsertTapInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return in.Next.Init(&w28bUpsertTapOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, tap: in.tap})
}

type w28bUpsertTapOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	tap *w28bUpsertTap
}

func (o *w28bUpsertTapOutbound) UpsertTypedSearchAttributes(ctx workflow.Context, attributes ...temporal.SearchAttributeUpdate) error {
	o.tap.calls = append(o.tap.calls, attributes)
	return o.Next.UpsertTypedSearchAttributes(ctx, attributes...)
}

type w28bAttributeRead struct {
	Tenant, Status, Version, Subject, BusinessKey                string
	TenantSet, StatusSet, VersionSet, SubjectSet, BusinessKeySet bool
}

func TestUpsertInstanceSearchAttributes_OneUpsert_CarriesAllFiveKeywordsWithTheGivenValues(t *testing.T) {
	cases := []struct {
		name                                          string
		tenant, status, version, subject, businessKey string
	}{
		{name: "every value set", tenant: "tenant-w28b", status: "RUNNING", version: "version-w28b", subject: "subject-w28b", businessKey: "order-w28b"},
		{name: "empty values are written as given", tenant: "tenant-w28b", status: "", version: "", subject: "", businessKey: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tap := &w28bUpsertTap{}
			wf := func(ctx workflow.Context) (w28bAttributeRead, error) {
				var out w28bAttributeRead
				if err := observability.UpsertInstanceSearchAttributes(ctx, tc.tenant, tc.status, tc.version, tc.subject, tc.businessKey); err != nil {
					return out, err
				}
				current := workflow.GetTypedSearchAttributes(ctx)
				out.Tenant, out.TenantSet = current.GetKeyword(observability.TenantIDKey)
				out.Status, out.StatusSet = current.GetKeyword(observability.InstanceStatusKey)
				out.Version, out.VersionSet = current.GetKeyword(observability.WorkflowVersionIDKey)
				out.Subject, out.SubjectSet = current.GetKeyword(observability.SubjectIDKey)
				out.BusinessKey, out.BusinessKeySet = current.GetKeyword(observability.BusinessKeyKey)
				return out, nil
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "w28bUpsertWorkflow"})
			env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{tap}})

			env.ExecuteWorkflow("w28bUpsertWorkflow")

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			var got w28bAttributeRead
			require.NoError(t, env.GetWorkflowResult(&got))
			assert.Equal(t, w28bAttributeRead{
				Tenant: tc.tenant, Status: tc.status, Version: tc.version, Subject: tc.subject, BusinessKey: tc.businessKey,
				TenantSet: true, StatusSet: true, VersionSet: true, SubjectSet: true, BusinessKeySet: true,
			}, got)

			require.Len(t, tap.calls, 1, "all five attributes go out in one upsert")
			sent := temporal.NewSearchAttributes(tap.calls[0]...)
			assert.Equal(t, 5, sent.Size())
			for key, want := range map[temporal.SearchAttributeKeyKeyword]string{
				observability.TenantIDKey: tc.tenant, observability.InstanceStatusKey: tc.status,
				observability.WorkflowVersionIDKey: tc.version, observability.SubjectIDKey: tc.subject,
				observability.BusinessKeyKey: tc.businessKey,
			} {
				value, ok := sent.GetKeyword(key)
				assert.Truef(t, ok, "%s must be in the upsert", key.GetName())
				assert.Equalf(t, want, value, "%s", key.GetName())
			}
		})
	}
}
