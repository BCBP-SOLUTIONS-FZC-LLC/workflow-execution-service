package grpc

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const InternalTokenMetadataKey = "x-internal-token"

// This surface is guarded by an internal-token check, not
// grpccommon.RequirePermission: this is machine-to-machine traffic
// (Definition Service calling Execution, no end user in the loop), and
// RequirePermission's Authorize(userID, roles, ...) model needs
// gateway-injected x-user-id/x-tenant-id/x-tenant-roles metadata that a
// service-to-service caller never sends. Matches the x-internal-token
// convention LLD §5.7/§9.2 already establish for this trust boundary.

var exemptFullMethodPrefixes = []string{
	"/grpc.health.v1.Health/",
	"/grpc.reflection.",
}

func isExemptMethod(fullMethod string) bool {
	for _, p := range exemptFullMethodPrefixes {
		if strings.HasPrefix(fullMethod, p) {
			return true
		}
	}
	return false
}

func UnaryRequireInternalToken(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" || isExemptMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		if !validInternalToken(ctx, token) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid x-internal-token metadata") //nolint:wrapcheck
		}
		return handler(ctx, req)
	}
}

func StreamRequireInternalToken(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if token == "" || isExemptMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		if !validInternalToken(ss.Context(), token) {
			return status.Error(codes.Unauthenticated, "missing or invalid x-internal-token metadata") //nolint:wrapcheck
		}
		return handler(srv, ss)
	}
}

func validInternalToken(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get(InternalTokenMetadataKey)
	if len(vals) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(vals[0]), []byte(token)) == 1
}
