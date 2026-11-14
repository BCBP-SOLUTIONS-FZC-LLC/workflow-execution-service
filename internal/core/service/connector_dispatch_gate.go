package service

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ConnectorDispatchGate = (*ConnectorDispatchGate)(nil)

// ConnectorDispatchGate keeps a connector from running for a task that is no
// longer open or whose instance has ended: a side effect such as an email
// cannot be undone by refusing the callback afterwards (execution LLD §6.2).
type ConnectorDispatchGate struct {
	Instances port.InstanceRepository
	Tasks     port.TaskRepository
}

// Dispatchable reports true for an unknown task: the callback refuses it, and
// refusing here would hide a task that is only late to commit.
func (g *ConnectorDispatchGate) Dispatchable(ctx context.Context, tenantID, taskID uuid.UUID) (bool, error) {
	task, err := g.Tasks.GetByID(ctx, tenantID, taskID)
	if errors.Is(err, domain.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, wrapTaskErr(err)
	}
	if task.Status != domain.TaskStatusReady && task.Status != domain.TaskStatusInProgress {
		return false, nil
	}
	inst, err := g.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return false, wrapInstanceErr(err)
	}
	ended := inst.Status == domain.InstanceStatusCompleted ||
		inst.Status == domain.InstanceStatusTerminated ||
		inst.Status == domain.InstanceStatusFailed
	return !ended, nil
}
