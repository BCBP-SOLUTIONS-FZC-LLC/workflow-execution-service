package temporalclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// wflContextSDK is a Temporal SDK client whose start call fails with a fixed
// error and whose update handle blocks until the caller's context ends.
type wflContextSDK struct {
	client.Client
	startErr error

	updateOptions client.UpdateWorkflowOptions
	waiting       chan struct{}
}

func (c *wflContextSDK) ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
	return nil, c.startErr
}

func (c *wflContextSDK) UpdateWorkflow(_ context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.updateOptions = opts
	return &wflBlockingHandle{waiting: c.waiting}, nil
}

type wflBlockingHandle struct {
	client.WorkflowUpdateHandle
	waiting chan struct{}
}

func (h *wflBlockingHandle) Get(ctx context.Context, _ interface{}) error {
	close(h.waiting)
	<-ctx.Done()
	return ctx.Err()
}

// A start that the SDK abandons at the caller's deadline comes back wrapped
// with the context error intact, so callers can still tell a timeout.
func TestClient_StartWorkflow_ADeadlineFromTheSDKStaysVisibleThroughTheWrap(t *testing.T) {
	c := temporalclient.New(&wflContextSDK{startErr: context.DeadlineExceeded})

	_, err := c.StartWorkflow(context.Background(), port.StartWorkflowInput{
		TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.Contains(t, err.Error(), "start workflow")
}

// An update whose caller's context ends while the workflow is still running it
// returns an error that wraps the context error; it is neither a not-found nor
// a refusal, and the update was asked to wait for completion.
func TestClient_UpdateWorkflow_ContextEndingWhileTheUpdateRunsIsReportedAsTheContextError(t *testing.T) {
	sdk := &wflContextSDK{waiting: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	instanceID := uuid.New()

	err := temporalclient.New(sdk).UpdateWorkflow(ctx, "tenant:biz", instanceID, "stage-defer", struct{}{})

	select {
	case <-sdk.waiting:
	default:
		t.Fatal("the update was never waited on, so the context did not end while it ran")
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrUpdateRefused)
	assert.Equal(t, "tenant:biz", sdk.updateOptions.WorkflowID)
	assert.Equal(t, "stage-defer:"+instanceID.String(), sdk.updateOptions.UpdateName)
	assert.Equal(t, client.WorkflowUpdateStageCompleted, sdk.updateOptions.WaitForStage)
}
