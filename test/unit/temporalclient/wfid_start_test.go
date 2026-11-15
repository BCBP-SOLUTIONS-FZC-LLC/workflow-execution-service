package temporalclient_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// An execution id belongs to one instance, so the run the SDK hands back for an id that is
// already open is that instance's own earlier start, and is taken without reading its history.
func TestClient_StartWorkflow_ARepeatStartTakesTheOpenRunOfItsOwnID(t *testing.T) {
	var got client.StartWorkflowOptions
	c := temporalclient.New(&fakeSDKClient{
		executeWorkflowFunc: func(_ context.Context, options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
			got = options
			return &fakeWorkflowRun{id: options.ID, runID: "run-open"}, nil
		},
	})
	id := "tenant:" + uuid.NewString() + ":instance:" + uuid.NewString()

	out, err := c.StartWorkflow(context.Background(), port.StartWorkflowInput{
		TemporalWorkflowID: id, TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	})

	require.NoError(t, err)
	assert.Equal(t, id, out.TemporalWorkflowID)
	assert.Equal(t, "run-open", out.TemporalRunID)
	assert.False(t, got.WorkflowExecutionErrorWhenAlreadyStarted, "the open run of this instance's own id is returned, not reported as a clash")
}
