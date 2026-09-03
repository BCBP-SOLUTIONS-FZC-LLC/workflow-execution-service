package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// callerDeptForCallPool labels the synthetic admin-stub task's department.
// CallPoolStep itself carries no department — the call is issued from "the
// main pool" as a whole (LLD §2.3) — so this is a fixed label, not a real
// DepartmentDef.ID lookup.
const callerDeptForCallPool = "call_pool"

// runCallPool dispatches a CallPool step (LLD §2.3). The theoretical
// workflow.ExecuteChildWorkflow mode (an independent-lifecycle CallPool) is
// intentionally not built here — no concrete fixture forces it today.
func (in *interpreter) runCallPool(ctx wf.Context, callerPlan *dsl.CompiledPlan, cp *dsl.CallPoolStep, admin wf.Channel) (domain.NodeKey, error) {
	target := findPlan(in.collab, cp.Pool)
	if target == nil {
		return "", fmt.Errorf("workflow: call_pool target plan %q not found", cp.Pool)
	}

	if target.Ignored {
		// Visit-qualified NodeID: two concurrent Parallel branches calling
		// the same Ignored pool would otherwise both resolve to the
		// identical NodeKey ("call_pool/"+cp.Pool), so the second
		// registration in stage.go's in.pending would silently clobber the
		// first's still-live entry — same "map keyed too coarsely" shape as
		// this package's own taskVisits convention (CLAUDE.md) exists to
		// prevent.
		in.callPoolVisits[cp.Pool]++
		adminStage := &dsl.StageDef{
			Type:     "approve",
			Activity: "admin_completed_pool:" + cp.Pool,
			NodeID:   fmt.Sprintf("%s#%d", cp.Pool, in.callPoolVisits[cp.Pool]),
			Role:     "tenant_admin",
		}
		return in.runStage(ctx, callerPlan, callerDeptForCallPool, adminStage)
	}

	out, err := in.runSteps(ctx, target, target.Execution.Steps, admin)
	return out.LastNode, err
}
