package service

import (
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type externalAnswerWire struct {
	Key       string
	Variables map[string]any
}

func (s *InstanceService) AnswerOutsideWait(ctx context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, key string, variables map[string]any) error {
	inst, err := s.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if !scope.IsAdmin {
		tasks, _, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{Limit: 100})
		if err != nil {
			return wrapTaskErr(err)
		}
		scope.Departments = nil
		if !s.callerInScope(ctx, tenantID, scope, tasks) {
			return port.ErrNotAuthorizedForRead
		}
	}
	compiled, err := s.fetchCompiledWorkflow(ctx, tenantID, inst.WorkflowVersionID)
	if err != nil {
		return err
	}
	collab, err := decodeCollaboration(compiled.CompiledPlanJSON)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(dsl.ExternalWaits(collab), func(w dsl.ExternalWait) bool { return w.Key == key }) {
		return port.ErrWaitNotFound
	}
	if err := validateInstanceStateFor(port.SignalInstanceExternalAnswer, inst.Status); err != nil {
		return err
	}

	var waiting []string
	if err := s.Temporal.QueryWorkflow(ctx, inst.TemporalWorkflowID, port.QueryWaitingExternal, &waiting); err != nil {
		return err
	}
	if !slices.Contains(waiting, key) {
		return port.ErrNotWaiting
	}
	return s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, port.SignalInstanceExternalAnswer, externalAnswerWire{
		Key: key, Variables: variables,
	})
}
