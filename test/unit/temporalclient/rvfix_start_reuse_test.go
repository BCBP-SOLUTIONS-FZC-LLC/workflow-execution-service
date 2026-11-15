package temporalclient_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func rvfixStartInput(id string) port.StartWorkflowInput {
	return port.StartWorkflowInput{
		TemporalWorkflowID: id, TaskQueue: "wf-queue", TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	}
}

func TestClient_StartWorkflow_NeverRerunsAnExecutionThatAlreadyClosed(t *testing.T) {
	var got client.StartWorkflowOptions
	c := temporalclient.New(&fakeSDKClient{
		executeWorkflowFunc: func(_ context.Context, options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
			got = options
			return &fakeWorkflowRun{id: options.ID, runID: "run-1"}, nil
		},
	})

	_, err := c.StartWorkflow(context.Background(), rvfixStartInput("tenant:t:instance:i"))

	require.NoError(t, err)
	assert.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, got.WorkflowIDReusePolicy,
		"a retried start of a finished instance must not begin it again from scratch")
}

func TestClient_StartWorkflow_AnAlreadyStartedExecutionIsASuccessWithItsRunID(t *testing.T) {
	c := temporalclient.New(&fakeSDKClient{
		executeWorkflowFunc: func(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
			return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "start-request", "run-earlier")
		},
	})

	out, err := c.StartWorkflow(context.Background(), rvfixStartInput("tenant:t:instance:i"))

	require.NoError(t, err, "the earlier start is the one the caller wanted")
	assert.Equal(t, "tenant:t:instance:i", out.TemporalWorkflowID)
	assert.Equal(t, "run-earlier", out.TemporalRunID)
}

func TestClient_StartWorkflow_AnotherRefusalStillFails(t *testing.T) {
	c := temporalclient.New(&fakeSDKClient{
		executeWorkflowFunc: func(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
			return nil, serviceerror.NewInvalidArgument("bad request")
		},
	})

	_, err := c.StartWorkflow(context.Background(), rvfixStartInput("tenant:t:instance:i"))

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
}
