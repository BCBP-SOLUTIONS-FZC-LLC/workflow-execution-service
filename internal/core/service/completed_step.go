package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func hasCompletedStep(ctx context.Context, tasks port.TaskRepository, tenantID, instanceID uuid.UUID) (bool, error) {
	var cursor *port.Cursor
	for {
		page, next, err := tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: cursor, Limit: 50})
		if err != nil {
			return false, fmt.Errorf("list tasks by instance: %w", err)
		}
		for _, t := range page {
			if t.Status == domain.TaskStatusCompleted {
				return true, nil
			}
		}
		if next == nil {
			return false, nil
		}
		cursor = next
	}
}
