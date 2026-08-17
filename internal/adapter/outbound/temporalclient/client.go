// Package temporalclient implements port.TemporalClient — the API process's
// own Temporal SDK client wrapper, wired only into cmd/server. A peer of
// internal/adapter/outbound/temporal (the Worker process's Activity-body
// package): the two serve opposite processes despite what design/LLD/
// execution_service.md §1.7's prose currently says about outbound/temporal's
// role — see that package's own doc comment.
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

// executeWorkflowInput mirrors internal/workflow.ExecuteInput field-for-field.
// This package cannot import internal/workflow (arch-lint's adapter/workflow
// peer-package rule, LLD §1.7), so it carries its own wire-compatible copy —
// Temporal's default JSON data converter matches by field name, not type
// identity, so this only stays correct as long as the two structs' field
// names agree. The testsuite.WorkflowTestSuite round-trip test in
// test/workflow/ exists specifically to catch drift between the two.
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

// SignalWorkflow constructs the exact channel name internal/workflow's signal
// router listens on (signalName + ":" + instanceID) — see port.TemporalClient's
// own doc comment for why temporalWorkflowID and instanceID must both be
// supplied, and must not be conflated.
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
