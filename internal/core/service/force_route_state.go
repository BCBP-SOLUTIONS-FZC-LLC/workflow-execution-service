package service

import (
	"context"
	"slices"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// checkForceRouteState refuses, before the signal, a force-route the workflow
// would drop: a failed branch exists only in a DEGRADED workflow, and the node
// history exists only in the running one (execution LLD §5.10).
func (s *InstanceService) checkForceRouteState(
	ctx context.Context, inst *domain.Instance, signalName, targetNodeKey string,
) error {
	degraded := inst.Status == domain.InstanceStatusDegraded
	if signalName == port.SignalInstanceForceFwd && !degraded {
		return nil
	}
	var state port.ForceRouteState
	err := s.Temporal.QueryWorkflow(ctx, inst.TemporalWorkflowID, port.QueryForceRouteState, &state)
	if err != nil {
		return err
	}
	if degraded {
		if !namesFailedBranch(state.FailedBranches, targetNodeKey) {
			return targetRefused("target %q names no failed branch", targetNodeKey)
		}
		return nil
	}
	if state.HistoryLen == 0 {
		return port.ErrForceBackNoSavedBranch
	}
	return nil
}

func namesFailedBranch(failed []port.FailedBranchState, targetNodeKey string) bool {
	if targetNodeKey == "" {
		return len(failed) == 1
	}
	targetDept, _ := deptAndSuffix(targetNodeKey)
	return slices.ContainsFunc(failed, func(fb port.FailedBranchState) bool {
		return fb.DeptID == targetDept || slices.Contains(fb.Covers, targetDept)
	})
}
