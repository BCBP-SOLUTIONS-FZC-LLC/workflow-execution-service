package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func (s *InstanceService) List(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
	repoFilter := port.InstanceListFilter{
		WorkflowVersionID: filter.WorkflowVersionID,
		StartedAfter:      filter.StartedAfter,
		StartedBefore:     filter.StartedBefore,
	}
	if filter.Status != nil {
		status := domain.InstanceStatus(*filter.Status)
		repoFilter.Status = &status
	}

	rows, next, err := s.Instances.ListByTenant(ctx, tenantID, repoFilter, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.Instance]{}, wrapInstanceErr(err)
	}
	items := make([]*port.Instance, len(rows))
	for i, inst := range rows {
		items[i] = toPortInstance(inst)
	}
	return port.PageResult[*port.Instance]{Items: items, NextCursor: encodeNextCursor(next)}, nil
}

func (s *InstanceService) Get(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope) (*port.Instance, []*port.Task, error) {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return nil, nil, wrapInstanceErr(err)
	}
	tasks, _, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
	if err != nil {
		return nil, nil, wrapTaskErr(err)
	}

	if !scope.IsAdmin {
		if !s.callerInScope(ctx, tenantID, scope, tasks) {
			return nil, nil, port.ErrNotAuthorizedForRead
		}
	}

	portTasks := make([]*port.Task, len(tasks))
	for i, t := range tasks {
		portTasks[i] = toPortTask(t)
	}
	return toPortInstance(inst), portTasks, nil
}

func (s *InstanceService) callerInScope(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, tasks []*domain.Task) bool {
	for _, t := range tasks {
		for _, d := range scope.Departments {
			if d.DepartmentID == t.DepartmentID {
				return true
			}
		}
		assignments, err := s.Assignments.ListActiveByTask(ctx, tenantID, t.ID)
		if err != nil {
			continue
		}
		for _, a := range assignments {
			if a.UserID == scope.CallerUserID {
				return true
			}
		}
	}
	return false
}

func (s *InstanceService) ListEvents(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, page port.Page) (port.PageResult[*port.WorkflowEvent], error) {
	if !scope.IsAdmin {
		tasks, _, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
		if err != nil {
			return port.PageResult[*port.WorkflowEvent]{}, wrapTaskErr(err)
		}
		if !s.callerInScope(ctx, tenantID, scope, tasks) {
			return port.PageResult[*port.WorkflowEvent]{}, port.ErrNotAuthorizedForRead
		}
	}

	rows, next, err := s.Outbox.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: pageAfter(page), Limit: page.Limit})
	if err != nil {
		return port.PageResult[*port.WorkflowEvent]{}, err
	}
	items := make([]*port.WorkflowEvent, 0, len(rows))
	for _, rec := range rows {
		event, err := toWorkflowEvent(rec, tenantID, instanceID)
		if err != nil {
			s.logger().Warn("skipping unrenderable outbox event", map[string]any{"event_id": rec.ID, "error": err.Error()})
			continue
		}
		items = append(items, event)
	}
	return port.PageResult[*port.WorkflowEvent]{Items: items, NextCursor: encodeNextCursor(next)}, nil
}
