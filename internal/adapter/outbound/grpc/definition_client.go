// Package grpc is the outbound Definition Service gRPC client (LLD §5.1, §5.3, port 9090).
package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	definitionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/definition/v1"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// ErrUpstreamUnavailable is retries-exhausted-or-cancelled-during-backoff —
// matches the LLD's own definition of UPSTREAM_UNAVAILABLE/503 ("failed
// after retries", §5.10). ErrUpstreamRejected is everything else: a
// non-retryable gRPC error returned on the very first attempt, or a
// malformed response — neither of those was ever "retried and stayed down".
var (
	ErrUpstreamUnavailable = errors.New("upstream service unavailable")
	ErrUpstreamRejected    = errors.New("upstream service rejected request")
)

// only codes.Unavailable/DeadlineExceeded are retried.
const maxCallAttempts = 3

// maxMessageSizeBytes matches the platform's HTTP body-size cap (LLD §5.10, §9.3).
const maxMessageSizeBytes = 10 << 20 // 10 MiB

var _ port.DefinitionServiceClient = (*DefinitionClient)(nil)

type DefinitionClient struct {
	client      definitionv1.DefinitionServiceClient
	conn        *grpc.ClientConn
	callTimeout time.Duration
}

func NewDefinitionClient(addr string, callTimeout time.Duration) (*DefinitionClient, error) {
	// Insecure credentials are intentional: intra-cluster traffic only; mTLS terminates at the service mesh.
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageSizeBytes),
			grpc.MaxCallSendMsgSize(maxMessageSizeBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial definition service: %w", err)
	}
	return &DefinitionClient{
		client:      definitionv1.NewDefinitionServiceClient(conn),
		conn:        conn,
		callTimeout: callTimeout,
	}, nil
}

func (c *DefinitionClient) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close definition client: %w", err)
	}
	return nil
}

func (c *DefinitionClient) GetCompiledWorkflow(
	ctx context.Context,
	tenantID, workflowVersionID uuid.UUID,
) (*port.CompiledWorkflow, error) {
	req := &definitionv1.GetCompiledWorkflowRequest{
		TenantId:          tenantID.String(),
		WorkflowVersionId: workflowVersionID.String(),
	}
	var lastErr error
	for attempt := 1; attempt <= maxCallAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, c.callTimeout)
		resp, callErr := c.client.GetCompiledWorkflow(attemptCtx, req)
		cancel()
		if callErr == nil {
			return parseCompiledWorkflow(resp)
		}
		if ctx.Err() != nil {
			// The caller's own context is already done — this is not the
			// upstream rejecting anything, it's the caller giving up.
			return nil, fmt.Errorf("%w: get compiled workflow: %w", ErrUpstreamUnavailable, ctx.Err())
		}
		if !isRetryableGRPC(callErr) {
			return nil, fmt.Errorf("%w: get compiled workflow: %w", ErrUpstreamRejected, callErr)
		}
		lastErr = callErr
		if attempt == maxCallAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: get compiled workflow: %w", ErrUpstreamUnavailable, ctx.Err())
		case <-time.After(backoff(attempt)):
		}
	}
	return nil, fmt.Errorf("%w: get compiled workflow: %w", ErrUpstreamUnavailable, lastErr)
}

func parseCompiledWorkflow(resp *definitionv1.GetCompiledWorkflowResponse) (*port.CompiledWorkflow, error) {
	wfID, err := uuid.Parse(resp.GetWorkflowId())
	if err != nil {
		return nil, fmt.Errorf("%w: parse workflow_id: %w", ErrUpstreamRejected, err)
	}
	verID, err := uuid.Parse(resp.GetVersionId())
	if err != nil {
		return nil, fmt.Errorf("%w: parse version_id: %w", ErrUpstreamRejected, err)
	}
	return &port.CompiledWorkflow{
		WorkflowID:       wfID,
		VersionID:        verID,
		VersionNumber:    resp.GetVersionNumber(),
		Status:           resp.GetStatus(),
		IsValid:          resp.GetIsValid(),
		CompiledPlanJSON: resp.GetCompiledPlanJson(),
	}, nil
}

func isRetryableGRPC(err error) bool {
	c := status.Code(err)
	return c == codes.Unavailable || c == codes.DeadlineExceeded
}

func backoff(attempt int) time.Duration {
	d := 50 * time.Millisecond << (attempt - 1)
	if d > 500*time.Millisecond {
		d = 500 * time.Millisecond
	}
	return d
}
