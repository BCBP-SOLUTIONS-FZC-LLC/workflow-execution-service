package temporalclient

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

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
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, port.WorkflowTypeExecute, executeWorkflowInput{
		TenantID:    in.TenantID.String(),
		InstanceID:  in.InstanceID.String(),
		VersionID:   in.WorkflowVersionID.String(),
		ContextJSON: in.ContextJSON,
		OverrideMap: in.OverrideMap,
	})
	if err != nil {
		return port.StartWorkflowOutput{}, fmt.Errorf("start workflow: %w", err)
	}
	return port.StartWorkflowOutput{TemporalWorkflowID: run.GetID(), TemporalRunID: run.GetRunID()}, nil
}

func (c *Client) SignalWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	channelName := signalName + ":" + instanceID.String()
	if err := c.sdk.SignalWorkflow(ctx, temporalWorkflowID, "", channelName, payload); err != nil {
		return fmt.Errorf("signal workflow %q: %w", channelName, err)
	}
	return nil
}

func (c *Client) TerminateWorkflow(ctx context.Context, temporalWorkflowID, reason string) error {
	if err := c.sdk.TerminateWorkflow(ctx, temporalWorkflowID, "", reason); err != nil {
		return fmt.Errorf("terminate workflow: %w", err)
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
