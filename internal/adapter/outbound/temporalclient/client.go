package temporalclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TemporalClient = (*Client)(nil)

type Client struct {
	sdk client.Client
}

func New(sdk client.Client) *Client {
	return &Client{sdk: sdk}
}

type executeWorkflowInput struct {
	TenantID    string
	InstanceID  string
	VersionID   string
	ContextJSON string
	OverrideMap map[string]string
}

func (c *Client) StartWorkflow(ctx context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	run, err := c.sdk.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    in.TemporalWorkflowID,
		TaskQueue:             in.TaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, port.WorkflowTypeExecute, executeWorkflowInput{
		TenantID:    in.TenantID.String(),
		InstanceID:  in.InstanceID.String(),
		VersionID:   in.WorkflowVersionID.String(),
		ContextJSON: in.ContextJSON,
		OverrideMap: in.OverrideMap,
	})
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &started) {
		return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: started.RunId}, nil
	}
	if err != nil {
		return port.StartWorkflowOutput{}, clusterFailure("start workflow", err)
	}
	return port.StartWorkflowOutput{TemporalWorkflowID: run.GetID(), TemporalRunID: run.GetRunID()}, nil
}

// clusterFailure marks only a failure a later attempt can cure as port.ErrTemporalUnavailable
// (callers answer 503). A refusal the cluster gives for the request itself stays a plain error.
func clusterFailure(op string, err error) error {
	if retryable(err) {
		return fmt.Errorf("%s: %w: %w", op, port.ErrTemporalUnavailable, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}

func retryable(err error) bool {
	var serviceErr serviceerror.ServiceError
	if !errors.As(err, &serviceErr) {
		return true // a transport error or the caller's own deadline
	}
	var (
		unavailable *serviceerror.Unavailable
		nsDown      *serviceerror.NamespaceUnavailable
		deadline    *serviceerror.DeadlineExceeded
		exhausted   *serviceerror.ResourceExhausted
		aborted     *serviceerror.Aborted
		internal    *serviceerror.Internal
	)
	return errors.As(err, &unavailable) || errors.As(err, &nsDown) || errors.As(err, &deadline) || errors.As(err, &exhausted) ||
		errors.As(err, &aborted) || errors.As(err, &internal)
}

func (c *Client) SignalWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	channelName := signalName + ":" + instanceID.String()
	if err := c.sdk.SignalWorkflow(ctx, temporalWorkflowID, "", channelName, payload); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("signal workflow %q: %w: %w", channelName, port.ErrWorkflowNotFound, err)
		}
		return clusterFailure(fmt.Sprintf("signal workflow %q", channelName), err)
	}
	return nil
}

func (c *Client) UpdateWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, updateName string, payload any) error {
	name := updateName + ":" + instanceID.String()
	handle, err := c.sdk.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   temporalWorkflowID,
		UpdateName:   name,
		Args:         []any{payload},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		err = handle.Get(ctx, nil)
	}
	if err == nil {
		return nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("update workflow %q: %w: %w", name, port.ErrWorkflowNotFound, err)
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == port.UpdateRefusedErrorType {
		return fmt.Errorf("%w: %s", port.ErrUpdateRefused, appErr.Message())
	}
	return fmt.Errorf("update workflow %q: %w", name, err)
}

func (c *Client) TerminateWorkflow(ctx context.Context, temporalWorkflowID, runID, reason string) error {
	err := c.sdk.TerminateWorkflow(ctx, temporalWorkflowID, runID, reason)
	var notFound *serviceerror.NotFound
	if err != nil && !errors.As(err, &notFound) {
		return clusterFailure("terminate workflow", err)
	}
	return nil
}

func (c *Client) QueryWorkflow(ctx context.Context, temporalWorkflowID, queryType string, out any) error {
	val, err := c.sdk.QueryWorkflow(ctx, temporalWorkflowID, "", queryType)
	if err != nil {
		return fmt.Errorf("query workflow: %w", err)
	}
	if err := val.Get(out); err != nil {
		return fmt.Errorf("decode query workflow result: %w", err)
	}
	return nil
}
