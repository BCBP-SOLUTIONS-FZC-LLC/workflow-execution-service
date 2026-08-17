package service

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.UserSafetyNetReconciler = (*UserSafetyNetReconciler)(nil)

// UserSafetyNetReconciler implements port.UserSafetyNetReconciler (LLD §6.2
// item 3): UserDeleted's tenant-wide, per-assignment vacate — not
// instance-wide, so a multi-assignee task's instance survives if a
// co-assignee remains active on it. No outbound event: §6.4's catalogue has
// no dedicated wire type for this trigger, unlike the reassign/reroute
// paths this package's other reconcilers drive.
type UserSafetyNetReconciler struct {
	Assignments port.TaskAssignmentRepository
}

func (s *UserSafetyNetReconciler) VacateAssignments(ctx context.Context, in port.UserDeletedInput) error {
	_, err := s.Assignments.VacateAllActiveByUser(ctx, in.TenantID, in.UserID)
	return err
}
