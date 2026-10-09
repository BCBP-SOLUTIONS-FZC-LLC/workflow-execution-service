package service

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.UserSafetyNetReconciler = (*UserSafetyNetReconciler)(nil)

type UserSafetyNetReconciler struct {
	Assignments port.TaskAssignmentRepository
}

func (s *UserSafetyNetReconciler) VacateAssignments(ctx context.Context, in port.UserDeletedInput) error {
	_, err := s.Assignments.VacateAllActiveByUser(ctx, in.TenantID, in.UserID)
	return err
}
