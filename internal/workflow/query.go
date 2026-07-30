package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// QueryGetWorkflowStatus is the LLD §3.1 Query name — the one Temporal
// Query this service defines. It is never exposed via an HTTP endpoint
// (execution_service.md line 972: the dashboard reads the Postgres
// projection instead, to avoid a second, Temporal-latency-bound read path);
// it exists for internal reconciliation and test tooling only. A Query
// handler can only be registered from inside the workflow function itself
// (workflow.SetQueryHandler), so this must live in this package — no
// sibling task can add it later without editing internal/workflow again.
const QueryGetWorkflowStatus = "get-workflow-status"

// WorkflowStatusQuery mirrors LLD §3.1's {status, current_node_keys,
// active_tasks[], saved_sibling_branches[]} shape. ActiveTasks reports the
// same NodeKeys as CurrentNodeKeys — this interpreter doesn't separately
// track richer per-task metadata (TaskID isn't retained after
// CreateTaskActivity returns); reconcile with the real shape once a sibling
// task needs more than NodeKey identity here.
type WorkflowStatusQuery struct {
	Status               domain.InstanceStatus
	CurrentNodeKeys      []domain.NodeKey
	ActiveTasks          []domain.NodeKey
	SavedSiblingBranches []string
}

// registerStatusQuery wires QueryGetWorkflowStatus. The handler only reads
// workflow-local state and returns it — no activities, no signals, no
// commands — so it's safe to call at any point during replay regardless of
// map iteration order (queries are never recorded into workflow history).
func (in *interpreter) registerStatusQuery(ctx wf.Context) error {
	err := wf.SetQueryHandler(ctx, QueryGetWorkflowStatus, func() (WorkflowStatusQuery, error) {
		keys := make([]domain.NodeKey, 0, len(in.pending))
		for k := range in.pending {
			keys = append(keys, k)
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
