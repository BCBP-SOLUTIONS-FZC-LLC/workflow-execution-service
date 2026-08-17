package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ArchiveGuard = (*ArchiveGuard)(nil)

// ArchiveGuard implements port.ArchiveGuard — the inbound gRPC
// CheckActiveInstances RPC Definition Service calls before archiving a
// workflow (LLD §5.3).
type ArchiveGuard struct {
	Instances port.InstanceRepository
}

func (s *ArchiveGuard) CheckActiveInstances(ctx context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error) {
	count, err := s.Instances.CountActiveByWorkflow(ctx, tenantID, workflowID)
	if err != nil {
		return false, 0, err
	}
	return count > 0, int32(count), nil //nolint:gosec
}
