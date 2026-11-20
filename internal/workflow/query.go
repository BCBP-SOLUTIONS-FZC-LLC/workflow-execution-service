package workflow

import (
	"fmt"
	"slices"
	"sort"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const QueryGetWorkflowStatus = "get-workflow-status"

const QueryWaitingExternal = "waiting-external"

type WorkflowStatusQuery struct {
	Status               domain.InstanceStatus
	CurrentNodeKeys      []domain.NodeKey
	ActiveTasks          []domain.NodeKey
	SavedSiblingBranches []string
	// UnassignedTasks lists the open tasks that lost their last active
	// assignee and have not been given a new one; the instance waits on them
	// (execution LLD §3.1).
	UnassignedTasks []string
}

func (in *interpreter) registerForceRouteStateQuery(ctx wf.Context) error {
	err := wf.SetQueryHandler(ctx, port.QueryForceRouteState, func() (port.ForceRouteState, error) {
		state := port.ForceRouteState{HistoryLen: len(in.history.stack)}
		for _, p := range in.liveParks() {
			for _, fb := range p.failed {
				state.FailedBranches = append(state.FailedBranches, failedBranchState(fb))
			}
		}
		return state, nil
	})
	if err != nil {
		return fmt.Errorf("workflow: registering %s query handler: %w", port.QueryForceRouteState, err)
	}
	return nil
}

func failedBranchState(fb failedBranch) port.FailedBranchState {
	covers := appendStepDepts([]string{fb.DeptID}, fb.Steps)
	if dept := deptFromNodeKey(fb.LastCompletedNode); dept != "" && !slices.Contains(covers, dept) {
		covers = append(covers, dept)
	}
	return port.FailedBranchState{DeptID: fb.DeptID, Covers: covers}
}

func (in *interpreter) registerStatusQuery(ctx wf.Context) error {
	err := wf.SetQueryHandler(ctx, QueryGetWorkflowStatus, func() (WorkflowStatusQuery, error) {
		keys := make([]domain.NodeKey, 0, len(in.pending))
		for k := range in.pending {
			keys = append(keys, k.Node)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		saved := make([]string, 0, len(in.pauseGates))
		for dept := range in.pauseGates {
			saved = append(saved, dept)
		}
		sort.Strings(saved)
		return WorkflowStatusQuery{
			Status:               in.status,
			CurrentNodeKeys:      keys,
			ActiveTasks:          keys,
			SavedSiblingBranches: saved,
			UnassignedTasks:      in.unassignedTasks(),
		}, nil
	})
	if err != nil {
		return fmt.Errorf("workflow: registering %s query handler: %w", QueryGetWorkflowStatus, err)
	}
	return nil
}
