package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func (s *InstanceService) List(ctx context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
	repoFilter := port.InstanceListFilter{
		SubjectID:         filter.SubjectID,
		WorkflowVersionID: filter.WorkflowVersionID,
		StartedAfter:      filter.StartedAfter,
		StartedBefore:     filter.StartedBefore,
	}
	if filter.Status != nil {
		status := domain.InstanceStatus(*filter.Status)
		repoFilter.Status = &status
	}
	if !scope.IsAdmin {
		repoFilter.Scope = readScopeFilter(scope)
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
	if err := s.requireVisible(ctx, tenantID, instanceID, scope); err != nil {
		return nil, nil, err
	}
	tasks, err := s.allTasks(ctx, tenantID, instanceID)
	if err != nil {
		return nil, nil, err
	}

	portTasks := make([]*port.Task, len(tasks))
	for i, t := range tasks {
		portTasks[i] = toPortTask(t)
	}
	return toPortInstance(inst), portTasks, nil
}

func (s *InstanceService) allTasks(ctx context.Context, tenantID, instanceID uuid.UUID) ([]*domain.Task, error) {
	var all []*domain.Task
	var after *port.Cursor
	for {
		page, next, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: after, Limit: 100})
		if err != nil {
			return nil, wrapTaskErr(err)
		}
		all = append(all, page...)
		if next == nil {
			return all, nil
		}
		after = next
	}
}

func (s *InstanceService) requireVisible(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope) error {
	if scope.IsAdmin {
		return nil
	}
	rows, _, err := s.Instances.ListByTenant(ctx, tenantID, port.InstanceListFilter{InstanceID: &instanceID, Scope: readScopeFilter(scope)}, port.PageRequest{Limit: 1})
	if err != nil {
		return wrapInstanceErr(err)
	}
	if len(rows) == 0 {
		return port.ErrNotAuthorizedForRead
	}
	return nil
}

func (s *InstanceService) ListEvents(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, page port.Page) (port.PageResult[*port.WorkflowEvent], error) {
	if err := s.requireVisible(ctx, tenantID, instanceID, scope); err != nil {
		return port.PageResult[*port.WorkflowEvent]{}, err
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
