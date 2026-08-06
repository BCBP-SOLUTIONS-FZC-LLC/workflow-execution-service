// Package grpc is the inbound ExecutionService gRPC server (LLD §5.3, port
// 9090): CheckActiveInstances, PauseUserTasks, and health-check registration.
package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	executionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/execution/v1"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type Server struct {
	executionv1.UnimplementedExecutionServiceServer
	log    port.Logger
	guard  port.ArchiveGuard
	pauser port.UserTaskPauser
}

func NewServer(log port.Logger, guard port.ArchiveGuard, pauser port.UserTaskPauser) *Server {
	return &Server{log: log, guard: guard, pauser: pauser}
}

func (s *Server) CheckActiveInstances(
	ctx context.Context,
	req *executionv1.CheckActiveInstancesRequest,
) (*executionv1.CheckActiveInstancesResponse, error) {
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required") //nolint:wrapcheck
	}
	if req.GetWorkflowId() == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_id is required") //nolint:wrapcheck
	}
	tenantID, err := uuid.Parse(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant_id") //nolint:wrapcheck
	}
	workflowID, err := uuid.Parse(req.GetWorkflowId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid workflow_id") //nolint:wrapcheck
	}

	hasActive, count, err := s.guard.CheckActiveInstances(ctx, tenantID, workflowID)
	if err != nil {
		s.log.Error("CheckActiveInstances: guard error", map[string]any{"error": err.Error()})
		return nil, status.Error(codes.Internal, "internal error") //nolint:wrapcheck
	}
	return &executionv1.CheckActiveInstancesResponse{HasActive: hasActive, Count: count}, nil
}

func (s *Server) PauseUserTasks(
	ctx context.Context,
	req *executionv1.PauseUserTasksRequest,
) (*executionv1.PauseUserTasksResponse, error) {
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required") //nolint:wrapcheck
	}
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required") //nolint:wrapcheck
	}
	tenantID, err := uuid.Parse(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant_id") //nolint:wrapcheck
	}
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id") //nolint:wrapcheck
	}

	if err := s.pauser.PauseUserTasks(ctx, tenantID, userID); err != nil {
		s.log.Error("PauseUserTasks: pauser error", map[string]any{"error": err.Error()})
		return nil, status.Error(codes.Internal, "internal error") //nolint:wrapcheck
	}
	return &executionv1.PauseUserTasksResponse{}, nil
}
