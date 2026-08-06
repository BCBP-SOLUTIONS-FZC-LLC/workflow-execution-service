// NewGRPCServer builds a fully wired *grpc.Server for this service's inbound
// gRPC surface (port 9090, LLD §5.3): the ExecutionService RPCs plus
// grpc_health_v1.Health/Check, wrapped in the platform's observability
// interceptor chain plus this package's own internal-token interceptor (see
// internalauth.go for the auth-model rationale). internalToken == "" disables
// the check (dev mode), matching the HTTP middleware's own convention.
package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/grpccommon"

	executionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/execution/v1"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// maxMessageSizeBytes matches the platform's HTTP body-size cap (LLD §5.10, §9.3).
const maxMessageSizeBytes = 10 << 20 // 10 MiB

func NewGRPCServer(
	serviceName, buildVersion, internalToken string,
	log port.Logger,
	guard port.ArchiveGuard,
	pauser port.UserTaskPauser,
) *grpc.Server {
	grpcCfg := grpccommon.Config{ServiceName: serviceName, BuildVersion: buildVersion, Logger: log}

	unary := append(
		grpccommon.ObservabilityUnaryInterceptors(grpcCfg),
		UnaryRequireInternalToken(internalToken),
	)
	stream := append(
		grpccommon.ObservabilityStreamInterceptors(grpcCfg),
		StreamRequireInternalToken(internalToken),
	)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
		grpc.MaxRecvMsgSize(maxMessageSizeBytes),
		grpc.MaxSendMsgSize(maxMessageSizeBytes),
	)
	executionv1.RegisterExecutionServiceServer(srv, NewServer(log, guard, pauser))
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv)
	return srv
}
