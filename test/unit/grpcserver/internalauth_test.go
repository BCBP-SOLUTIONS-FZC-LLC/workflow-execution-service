package grpcserver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	grpcadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/grpc"
)

func callThrough(ctx context.Context, interceptor grpc.UnaryServerInterceptor, fullMethod string) (bool, error) {
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)
	return handlerCalled, err
}

func TestUnaryRequireInternalToken_EmptyToken_PassesThrough(t *testing.T) {
	interceptor := grpcadapter.UnaryRequireInternalToken("")

	called, err := callThrough(context.Background(), interceptor, "/workflow.execution.v1.ExecutionService/CheckActiveInstances")

	require.NoError(t, err)
	assert.True(t, called)
}

func TestUnaryRequireInternalToken_MatchingMetadata_PassesThrough(t *testing.T) {
	interceptor := grpcadapter.UnaryRequireInternalToken("secret-token")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(grpcadapter.InternalTokenMetadataKey, "secret-token"))

	called, err := callThrough(ctx, interceptor, "/workflow.execution.v1.ExecutionService/CheckActiveInstances")

	require.NoError(t, err)
	assert.True(t, called)
}

func TestUnaryRequireInternalToken_MissingMetadata_Unauthenticated(t *testing.T) {
	interceptor := grpcadapter.UnaryRequireInternalToken("secret-token")

	called, err := callThrough(context.Background(), interceptor, "/workflow.execution.v1.ExecutionService/CheckActiveInstances")

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, called)
}

func TestUnaryRequireInternalToken_MismatchedMetadata_Unauthenticated(t *testing.T) {
	interceptor := grpcadapter.UnaryRequireInternalToken("secret-token")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(grpcadapter.InternalTokenMetadataKey, "wrong-token"))

	called, err := callThrough(ctx, interceptor, "/workflow.execution.v1.ExecutionService/CheckActiveInstances")

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, called)
}

func TestUnaryRequireInternalToken_HealthCheckExempt(t *testing.T) {
	interceptor := grpcadapter.UnaryRequireInternalToken("secret-token")

	called, err := callThrough(context.Background(), interceptor, "/grpc.health.v1.Health/Check")

	require.NoError(t, err)
	assert.True(t, called)
}

func TestUnaryRequireInternalToken_ReflectionExempt(t *testing.T) {
	interceptor := grpcadapter.UnaryRequireInternalToken("secret-token")

	called, err := callThrough(context.Background(), interceptor, "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")

	require.NoError(t, err)
	assert.True(t, called)
}

func streamCallThrough(ctx context.Context, interceptor grpc.StreamServerInterceptor, fullMethod string) (bool, error) {
	handlerCalled := false
	handler := func(srv any, ss grpc.ServerStream) error {
		handlerCalled = true
		return nil
	}
	err := interceptor(nil, &fakeServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: fullMethod}, handler)
	return handlerCalled, err
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func TestStreamRequireInternalToken_MatchingMetadata_PassesThrough(t *testing.T) {
	interceptor := grpcadapter.StreamRequireInternalToken("secret-token")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(grpcadapter.InternalTokenMetadataKey, "secret-token"))

	called, err := streamCallThrough(ctx, interceptor, "/workflow.execution.v1.ExecutionService/SomeStream")

	require.NoError(t, err)
	assert.True(t, called)
}

func TestStreamRequireInternalToken_MissingMetadata_Unauthenticated(t *testing.T) {
	interceptor := grpcadapter.StreamRequireInternalToken("secret-token")

	called, err := streamCallThrough(context.Background(), interceptor, "/workflow.execution.v1.ExecutionService/SomeStream")

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, called)
}

func TestStreamRequireInternalToken_HealthCheckExempt(t *testing.T) {
	interceptor := grpcadapter.StreamRequireInternalToken("secret-token")

	called, err := streamCallThrough(context.Background(), interceptor, "/grpc.health.v1.Health/Watch")

	require.NoError(t, err)
	assert.True(t, called)
}

func TestStreamRequireInternalToken_ReflectionExempt(t *testing.T) {
	interceptor := grpcadapter.StreamRequireInternalToken("secret-token")

	called, err := streamCallThrough(context.Background(), interceptor, "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")

	require.NoError(t, err)
	assert.True(t, called)
}
