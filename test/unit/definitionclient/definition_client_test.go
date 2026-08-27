// Package definitionclient_test mirrors definition_service's own
// test/unit/executionclient wire-level pattern: a real in-process
// grpc.Server driving the real client, no mocked interfaces.
package definitionclient_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	definitionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/definition/v1"
	grpcadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const defaultTimeout = 5 * time.Second

type fakeDefinitionServer struct {
	definitionv1.UnimplementedDefinitionServiceServer
	resp *definitionv1.GetCompiledWorkflowResponse
	err  error
}

func (f *fakeDefinitionServer) GetCompiledWorkflow(
	_ context.Context,
	_ *definitionv1.GetCompiledWorkflowRequest,
) (*definitionv1.GetCompiledWorkflowResponse, error) {
	return f.resp, f.err
}

type flakyDefinitionServer struct {
	definitionv1.UnimplementedDefinitionServiceServer
	failCount int
	calls     int
	resp      *definitionv1.GetCompiledWorkflowResponse
}

func (f *flakyDefinitionServer) GetCompiledWorkflow(
	_ context.Context,
	_ *definitionv1.GetCompiledWorkflowRequest,
) (*definitionv1.GetCompiledWorkflowResponse, error) {
	f.calls++
	if f.calls <= f.failCount {
		return nil, status.Error(codes.Unavailable, "transient")
	}
	return f.resp, nil
}

type slowDefinitionServer struct {
	definitionv1.UnimplementedDefinitionServiceServer
}

func (s *slowDefinitionServer) GetCompiledWorkflow(
	ctx context.Context,
	_ *definitionv1.GetCompiledWorkflowRequest,
) (*definitionv1.GetCompiledWorkflowResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// countingServer verifies non-retryable errors do not trigger retries.
type countingServer struct {
	definitionv1.UnimplementedDefinitionServiceServer
	calls int
	code  codes.Code
}

func (s *countingServer) GetCompiledWorkflow(
	_ context.Context,
	_ *definitionv1.GetCompiledWorkflowRequest,
) (*definitionv1.GetCompiledWorkflowResponse, error) {
	s.calls++
	return nil, status.Error(s.code, "error")
}

func startServer(t *testing.T, srv definitionv1.DefinitionServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	definitionv1.RegisterDefinitionServiceServer(s, srv)
	go s.Serve(lis) //nolint:errcheck
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func TestNewDefinitionClient_AndClose(t *testing.T) {
	addr := startServer(t, &fakeDefinitionServer{})
	c, err := grpcadapter.NewDefinitionClient(addr, defaultTimeout)
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

// A target containing a control character fails net/url parsing before any
// network dial is attempted — a real, deterministic way to exercise
// NewDefinitionClient's dial-setup error path without a live server.
func TestNewDefinitionClient_DialError(t *testing.T) {
	_, err := grpcadapter.NewDefinitionClient("bad\x7ftarget", defaultTimeout)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dial definition service")
}

func TestDefinitionClient_Close_AlreadyClosed(t *testing.T) {
	addr := startServer(t, &fakeDefinitionServer{})
	c, err := grpcadapter.NewDefinitionClient(addr, defaultTimeout)
	require.NoError(t, err)
	require.NoError(t, c.Close())

	err = c.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "close definition client")
}

func TestDefinitionClient_GetCompiledWorkflow(t *testing.T) {
	wfID := uuid.New()
	verID := uuid.New()

	tests := []struct {
		name          string
		srv           definitionv1.DefinitionServiceServer
		clientTimeout time.Duration
		makeCtx       func(t *testing.T) context.Context
		wantErr       bool
		wantSentinel  error
		want          *port.CompiledWorkflow
		checkServer   func(t *testing.T, srv definitionv1.DefinitionServiceServer)
		checkElapsed  func(t *testing.T, elapsed time.Duration)
	}{
		{
			name: "success",
			srv: &fakeDefinitionServer{resp: &definitionv1.GetCompiledWorkflowResponse{
				WorkflowId:       wfID.String(),
				VersionId:        verID.String(),
				VersionNumber:    3,
				Status:           "PUBLISHED",
				IsValid:          true,
				CompiledPlanJson: `{"nodes":[]}`,
			}},
			want: &port.CompiledWorkflow{
				WorkflowID:       wfID,
				VersionID:        verID,
				VersionNumber:    3,
				Status:           "PUBLISHED",
				IsValid:          true,
				CompiledPlanJSON: `{"nodes":[]}`,
			},
		},
		{
			name: "draft with zero version number",
			srv: &fakeDefinitionServer{resp: &definitionv1.GetCompiledWorkflowResponse{
				WorkflowId: wfID.String(),
				VersionId:  verID.String(),
				Status:     "DRAFT",
			}},
			want: &port.CompiledWorkflow{
				WorkflowID: wfID,
				VersionID:  verID,
				Status:     "DRAFT",
			},
		},
		{
			name:         "upstream error",
			srv:          &fakeDefinitionServer{err: status.Error(codes.Internal, "internal error")},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamRejected,
		},
		{
			name:          "call timeout",
			srv:           &slowDefinitionServer{},
			clientTimeout: 50 * time.Millisecond,
			wantErr:       true,
			wantSentinel:  grpcadapter.ErrUpstreamUnavailable,
			checkElapsed: func(t *testing.T, elapsed time.Duration) {
				assert.LessOrEqual(t, elapsed, time.Second)
			},
		},
		{
			name: "retries then succeeds",
			srv: &flakyDefinitionServer{
				failCount: 2,
				resp: &definitionv1.GetCompiledWorkflowResponse{
					WorkflowId: wfID.String(),
					VersionId:  verID.String(),
				},
			},
			want: &port.CompiledWorkflow{WorkflowID: wfID, VersionID: verID},
			checkServer: func(t *testing.T, srv definitionv1.DefinitionServiceServer) {
				assert.Equal(t, 3, srv.(*flakyDefinitionServer).calls)
			},
		},
		{
			name:         "non-retryable error makes only 1 attempt",
			srv:          &countingServer{code: codes.PermissionDenied},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamRejected,
			checkServer: func(t *testing.T, srv definitionv1.DefinitionServiceServer) {
				assert.Equal(t, 1, srv.(*countingServer).calls)
			},
		},
		{
			name: "context cancelled during backoff short-circuits retry",
			srv:  &countingServer{code: codes.Unavailable},
			makeCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				time.AfterFunc(30*time.Millisecond, cancel)
				return ctx
			},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamUnavailable,
			checkElapsed: func(t *testing.T, elapsed time.Duration) {
				assert.LessOrEqual(t, elapsed, 200*time.Millisecond)
			},
		},
		{
			name:         "retries exhausted without cancellation makes 3 attempts",
			srv:          &countingServer{code: codes.Unavailable},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamUnavailable,
			checkServer: func(t *testing.T, srv definitionv1.DefinitionServiceServer) {
				assert.Equal(t, 3, srv.(*countingServer).calls)
			},
		},
		{
			name: "context already cancelled before the first attempt is upstream-unavailable, not rejected",
			srv:  &countingServer{code: codes.Unavailable},
			makeCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamUnavailable,
		},
		{
			name:         "malformed workflow_id fails closed",
			srv:          &fakeDefinitionServer{resp: &definitionv1.GetCompiledWorkflowResponse{WorkflowId: "not-a-uuid", VersionId: verID.String()}},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamRejected,
		},
		{
			name:         "malformed version_id fails closed",
			srv:          &fakeDefinitionServer{resp: &definitionv1.GetCompiledWorkflowResponse{WorkflowId: wfID.String(), VersionId: "not-a-uuid"}},
			wantErr:      true,
			wantSentinel: grpcadapter.ErrUpstreamRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := startServer(t, tt.srv)
			timeout := defaultTimeout
			if tt.clientTimeout > 0 {
				timeout = tt.clientTimeout
			}
			c, err := grpcadapter.NewDefinitionClient(addr, timeout)
			require.NoError(t, err)
			defer c.Close() //nolint:errcheck

			ctx := context.Background()
			if tt.makeCtx != nil {
				ctx = tt.makeCtx(t)
			}

			start := time.Now()
			got, err := c.GetCompiledWorkflow(ctx, uuid.New(), uuid.New())
			elapsed := time.Since(start)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantSentinel != nil {
					assert.True(t, errors.Is(err, tt.wantSentinel), "expected error to wrap %v, got %v", tt.wantSentinel, err)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.checkElapsed != nil {
				tt.checkElapsed(t, elapsed)
			}
			if tt.checkServer != nil {
				tt.checkServer(t, tt.srv)
			}
		})
	}
}
