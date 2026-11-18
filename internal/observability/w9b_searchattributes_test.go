package observability_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

var errW9bUpsertRejected = errors.New("search attribute not registered")

// w9bRejectingUpserts stands in for the SDK refusing a search-attribute
// upsert: the workflow test environment itself never rejects one.
type w9bRejectingUpserts struct {
	interceptor.WorkerInterceptorBase
	attributesOffered int
}

func (r *w9bRejectingUpserts) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &w9bRejectingInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, owner: r}
}

type w9bRejectingInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	owner *w9bRejectingUpserts
}

func (i *w9bRejectingInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&w9bRejectingOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, owner: i.owner})
}

type w9bRejectingOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	owner *w9bRejectingUpserts
}

func (o *w9bRejectingOutbound) UpsertTypedSearchAttributes(_ workflow.Context, attributes ...temporal.SearchAttributeUpdate) error {
	o.owner.attributesOffered = len(attributes)
	return errW9bUpsertRejected
}

func TestUpsertInstanceSearchAttributes_SDKRejectsTheUpsert_ReturnsAWrappedCause(t *testing.T) {
	rejecting := &w9bRejectingUpserts{}
	var upsertErr error
	w9bUpsertWorkflow := func(ctx workflow.Context) error {
		upsertErr = observability.UpsertInstanceSearchAttributes(ctx, "tenant-1", "RUNNING", "version-1", "subject-1", "key-1")
		return nil
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(w9bUpsertWorkflow, workflow.RegisterOptions{Name: "w9bUpsertWorkflow"})
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{rejecting}})

	env.ExecuteWorkflow("w9bUpsertWorkflow")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, 5, rejecting.attributesOffered, "the upsert reached the SDK with every attribute")
	require.ErrorIs(t, upsertErr, errW9bUpsertRejected, "the caller can unwrap the SDK's cause")
	assert.EqualError(t, upsertErr, "upsert instance search attributes: search attribute not registered")
}
