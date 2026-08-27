package temporal_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	definitionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/definition/v1"
	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fakePlanServer struct {
	definitionv1.UnimplementedDefinitionServiceServer
	resp *definitionv1.GetCompiledWorkflowResponse
	err  error
}

func (f *fakePlanServer) GetCompiledWorkflow(
	context.Context, *definitionv1.GetCompiledWorkflowRequest,
) (*definitionv1.GetCompiledWorkflowResponse, error) {
	return f.resp, f.err
}

func startPlanServer(t *testing.T, srv definitionv1.DefinitionServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	definitionv1.RegisterDefinitionServiceServer(s, srv)
	go s.Serve(lis) //nolint:errcheck
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func TestGetCompiledPlan_Success(t *testing.T) {
	wfID, verID := uuid.New(), uuid.New()
	addr := startPlanServer(t, &fakePlanServer{resp: &definitionv1.GetCompiledWorkflowResponse{
		WorkflowId: wfID.String(), VersionId: verID.String(),
		CompiledPlanJson: `{"main_plan":"main","schema_version":1}`,
	}})
	definitions, err := outboundgrpc.NewDefinitionClient(addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = definitions.Close() })
	deps := &outboundtemporal.Deps{Definitions: definitions}

	out, err := deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{
		TenantID: uuid.New().String(), VersionID: verID.String(),
	})
	require.NoError(t, err)
	assert.Equal(t, "main", out.Collaboration.MainPlan)
}

func TestGetCompiledPlan_InvalidTenantID_IsNonRetryable(t *testing.T) {
	deps := &outboundtemporal.Deps{}
	_, err := deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: "not-a-uuid", VersionID: uuid.New().String()})
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, "DefinitionServiceClientError", appErr.Type())
}

func TestGetCompiledPlan_UpstreamRejected_IsNonRetryable(t *testing.T) {
	addr := startPlanServer(t, &fakePlanServer{err: errors.New("boom")})
	definitions, err := outboundgrpc.NewDefinitionClient(addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = definitions.Close() })
	deps := &outboundtemporal.Deps{Definitions: definitions}

	_, err = deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{
		TenantID: uuid.New().String(), VersionID: uuid.New().String(),
	})
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, "DefinitionServiceClientError", appErr.Type())
}

// fakeUnavailableServer always returns a retryable gRPC Unavailable status —
// unlike TestGetCompiledPlan_UpstreamRejected_IsNonRetryable's plain error
// (immediately classified ErrUpstreamRejected), this exhausts
// DefinitionClient's own retry budget and comes back wrapped in
// ErrUpstreamUnavailable instead, exercising GetCompiledPlan's other error
// branch: the plain (retryable) wrap, not nonRetryable.
type fakeUnavailableServer struct {
	definitionv1.UnimplementedDefinitionServiceServer
}

func (f *fakeUnavailableServer) GetCompiledWorkflow(
	context.Context, *definitionv1.GetCompiledWorkflowRequest,
) (*definitionv1.GetCompiledWorkflowResponse, error) {
	return nil, status.Error(codes.Unavailable, "definition_service is down")
}

func TestGetCompiledPlan_UpstreamUnavailable_IsRetryable(t *testing.T) {
	addr := startPlanServer(t, &fakeUnavailableServer{})
	definitions, err := outboundgrpc.NewDefinitionClient(addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = definitions.Close() })
	deps := &outboundtemporal.Deps{Definitions: definitions}

	_, err = deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{
		TenantID: uuid.New().String(), VersionID: uuid.New().String(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "get compiled workflow")
	var appErr *temporal.ApplicationError
	assert.False(t, errors.As(err, &appErr), "an exhausted-retry upstream-unavailable error must stay retryable, not become a nonRetryable ApplicationError")
}

func TestGetCompiledPlan_MalformedCompiledPlanJSON_IsNonRetryable(t *testing.T) {
	verID := uuid.New()
	addr := startPlanServer(t, &fakePlanServer{resp: &definitionv1.GetCompiledWorkflowResponse{
		WorkflowId: uuid.New().String(), VersionId: verID.String(), CompiledPlanJson: `not-json`,
	}})
	definitions, err := outboundgrpc.NewDefinitionClient(addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = definitions.Close() })
	deps := &outboundtemporal.Deps{Definitions: definitions}

	_, err = deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{
		TenantID: uuid.New().String(), VersionID: verID.String(),
	})
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, "DefinitionServiceClientError", appErr.Type())
}
