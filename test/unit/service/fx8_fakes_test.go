package service_test

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
