package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	taskTypeUser    = "userTask"
	taskTypeService = "serviceTask"
)

func taskTypeOf(task *domain.Task) string {
	if task.ConnectorType != nil && *task.ConnectorType != "" {
		return taskTypeService
	}
	return taskTypeUser
}

// toPortTasksWithCounts maps a page of tasks and fills each one's active assignee count with one query.
func toPortTasksWithCounts(ctx context.Context, assignments port.TaskAssignmentRepository, tenantID uuid.UUID, tasks []*domain.Task) ([]*port.Task, error) {
	out := make([]*port.Task, len(tasks))
	if len(tasks) == 0 {
		return out, nil
	}
	ids := make([]uuid.UUID, len(tasks))
	for i, t := range tasks {
		ids[i] = t.ID
	}
	counts, err := assignments.CountActiveByTasks(ctx, tenantID, ids)
	if err != nil {
		return nil, err
	}
	for i, t := range tasks {
		out[i] = toPortTask(t)
		out[i].AssigneeCount = counts[t.ID]
	}
	return out, nil
}
