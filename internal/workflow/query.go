package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

const QueryGetWorkflowStatus = "get-workflow-status"

const QueryWaitingExternal = "waiting-external"

type WorkflowStatusQuery struct {
	Status               domain.InstanceStatus
	CurrentNodeKeys      []domain.NodeKey
	ActiveTasks          []domain.NodeKey
	SavedSiblingBranches []string
}

func (in *interpreter) registerStatusQuery(ctx wf.Context) error {
	err := wf.SetQueryHandler(ctx, QueryGetWorkflowStatus, func() (WorkflowStatusQuery, error) {
		keys := make([]domain.NodeKey, 0, len(in.pending))
		for k := range in.pending {
			keys = append(keys, k.Node)
		}
		saved := make([]string, 0, len(in.pauseGates))
		for dept := range in.pauseGates {
			saved = append(saved, dept)
		}
		return WorkflowStatusQuery{
			Status:               in.status,
			CurrentNodeKeys:      keys,
			ActiveTasks:          keys,
			SavedSiblingBranches: saved,
		}, nil
	})
	if err != nil {
		return fmt.Errorf("workflow: registering %s query handler: %w", QueryGetWorkflowStatus, err)
	}
	return nil
}
