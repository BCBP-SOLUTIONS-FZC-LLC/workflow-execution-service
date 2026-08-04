package grpcserver_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	executionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/execution/v1"
	grpcadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/grpc"
)

func startGRPCServer(t *testing.T, internalToken string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpcadapter.NewGRPCServer("execution-service-test", "test", internalToken, &fakeLogger{}, &fakeArchiveGuard{}, &fakeUserTaskPauser{})
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck
	return conn
}

func TestNewGRPCServer_HealthCheck_UnauthenticatedEvenWithTokenConfigured(t *testing.T) {
	addr := startGRPCServer(t, "secret-token")
	conn := dial(t, addr)
	client := grpc_health_v1.NewHealthClient(conn)

	resp, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})

	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestNewGRPCServer_CheckActiveInstances_RejectsWithoutToken(t *testing.T) {
	addr := startGRPCServer(t, "secret-token")
	conn := dial(t, addr)
	client := executionv1.NewExecutionServiceClient(conn)

	_, err := client.CheckActiveInstances(context.Background(), &executionv1.CheckActiveInstancesRequest{
		TenantId:   "11111111-1111-1111-1111-111111111111",
		WorkflowId: "22222222-2222-2222-2222-222222222222",
	})

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestNewGRPCServer_CheckActiveInstances_SucceedsWithCorrectToken(t *testing.T) {
	addr := startGRPCServer(t, "secret-token")
	conn := dial(t, addr)
	client := executionv1.NewExecutionServiceClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), grpcadapter.InternalTokenMetadataKey, "secret-token")

	_, err := client.CheckActiveInstances(ctx, &executionv1.CheckActiveInstancesRequest{
		TenantId:   "11111111-1111-1111-1111-111111111111",
		WorkflowId: "22222222-2222-2222-2222-222222222222",
	})

	require.NoError(t, err)
}

func TestNewGRPCServer_CheckActiveInstances_SucceedsWithNoTokenConfigured(t *testing.T) {
	addr := startGRPCServer(t, "")
	conn := dial(t, addr)
	client := executionv1.NewExecutionServiceClient(conn)

	_, err := client.CheckActiveInstances(context.Background(), &executionv1.CheckActiveInstancesRequest{
		TenantId:   "11111111-1111-1111-1111-111111111111",
		WorkflowId: "22222222-2222-2222-2222-222222222222",
	})

	require.NoError(t, err)
}
