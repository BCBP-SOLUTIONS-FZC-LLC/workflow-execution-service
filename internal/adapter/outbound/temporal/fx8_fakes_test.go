package temporal_test

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func (r *fakeAssignmentRepo) ListByTaskAndUser(_ context.Context, _, taskID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.TaskID == taskID && a.UserID == userID {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r wkactAssignments) ListByTaskAndUser(_ context.Context, tenant, taskID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.w.record("Assignments.ListByTaskAndUser", tenant, taskID)
	var out []*domain.TaskAssignment
	for _, a := range r.w.assignments {
		if a.TenantID == tenant && a.TaskID == taskID && a.UserID == userID {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r w25bAssignments) ListByTaskAndUser(ctx context.Context, tenant, taskID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.ListByTaskAndUser", tenant, taskID, 0); err != nil {
		return nil, err
	}
	var out []*domain.TaskAssignment
	for _, id := range r.rig.assignmentOrder {
		if a := r.rig.assignments[id]; a.TenantID == tenant && a.TaskID == taskID && a.UserID == userID {
			cp := *a
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r w22aAssignments) ListByTaskAndUser(ctx context.Context, tenant, taskID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if err := r.rig.enter(ctx, "Assignments.ListByTaskAndUser", tenant, taskID, 0); err != nil {
		return nil, err
	}
	var out []*domain.TaskAssignment
	for _, id := range r.rig.assignmentOrder {
		if a := r.rig.assignments[id]; a.TenantID == tenant && a.TaskID == taskID && a.UserID == userID {
			cp := *a
			out = append(out, &cp)
		}
	}
	return out, nil
}
