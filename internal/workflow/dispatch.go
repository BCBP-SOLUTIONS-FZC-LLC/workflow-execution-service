package workflow

import (
	"fmt"
	"strings"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// runSteps is the recursive interpreter over a compiled ExecutionPlan's step
// list (execution LLD §2.5): dispatched in array order, purely on which
// ExecutionStep variant field is populated.
func (in *interpreter) runSteps(ctx wf.Context, plan *dsl.CompiledPlan, steps []dsl.ExecutionStep, admin wf.Channel) (stepOutcome, error) {
	var last domain.NodeKey
	for i := range steps {
		step := &steps[i]

		// IOMapping is populated only for callActivity inlining and applies
		// on entry to the inlined segment only — outputs are never
		// re-applied on exit (LLD §2.1).
		if step.IOMapping != nil {
			ctxJSON, err := applyIOMapping(in.contextJSON, step.IOMapping)
			if err != nil {
				return stepOutcome{LastNode: last}, err
			}
			in.contextJSON = ctxJSON
		}

		switch {
		case len(step.Sequential) > 0:
			for _, deptID := range step.Sequential {
				node, err := in.runDepartment(ctx, plan, deptID)
				last = node
				if err != nil {
					return stepOutcome{LastNode: last}, err
				}
			}

		case len(step.Parallel) > 0:
			out, err := in.runParallel(ctx, plan, step.Parallel, admin)
			last = out.LastNode
			if err != nil || out.Terminated {
				return out, err
			}

		case len(step.Exclusive) > 0:
			out, err := in.runExclusive(ctx, plan, step.Exclusive)
			last = out.LastNode
			if err != nil || out.Terminated {
				return out, err
			}

		case step.SubWorkflow != nil:
			node, err := in.runSubWorkflow(ctx, plan, step.SubWorkflow, admin)
			last = node
			if err != nil {
				return stepOutcome{LastNode: last}, err
			}

		case step.CallPool != nil:
			node, err := in.runCallPool(ctx, plan, step.CallPool, admin)
			last = node
			if err != nil {
				return stepOutcome{LastNode: last}, err
			}

		default:
			return stepOutcome{LastNode: last}, fmt.Errorf("workflow: execution step has no populated variant")
		}
	}
	return stepOutcome{LastNode: last}, nil
}

// runDepartment runs a department's stages (a flat []StageDef, LLD §2.5
// point 2.i) from the start.
func (in *interpreter) runDepartment(ctx wf.Context, plan *dsl.CompiledPlan, deptID string) (domain.NodeKey, error) {
	return in.runDepartmentFrom(ctx, plan, deptID, 0)
}

// spawnNonInterruptingTarget runs a non-interrupting boundary's TargetDept
// independently of the host it's attached to (LLD §2.2 step 5) — used by
// both runTaskStage (stage.go) and runSubWorkflow (subworkflow.go), which
// otherwise race the identical host-completion-vs-boundary Selector.
func (in *interpreter) spawnNonInterruptingTarget(ctx wf.Context, plan *dsl.CompiledPlan, targetDept string) {
	wf.Go(ctx, func(gctx wf.Context) {
		_, _ = in.runDepartment(gctx, plan, targetDept)
	})
}

// runDepartmentFrom also backs DEGRADED respawn (degraded.go), resuming a
// failed branch just past its last completed stage instead of from the top.
func (in *interpreter) runDepartmentFrom(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, startIdx int) (domain.NodeKey, error) {
	dept := findDepartment(plan, deptID)
	if dept == nil {
		return "", fmt.Errorf("workflow: department %q not found in plan %q", deptID, plan.Name)
	}
	var last domain.NodeKey
	for i := startIdx; i < len(dept.Stages); i++ {
		in.checkPaused(ctx, deptID)
		node, err := in.runStage(ctx, plan, deptID, &dept.Stages[i])
		if err != nil {
			// last stays the prior stage — stageIndexAfter resumes AFTER
			// whatever key is returned here; reporting the failed stage
			// itself would skip retrying it on respawn.
			return last, err
		}
		last = node
	}
	return last, nil
}

func findDepartment(plan *dsl.CompiledPlan, id string) *dsl.DepartmentDef {
	for i := range plan.Departments {
		if plan.Departments[i].ID == id {
			return &plan.Departments[i]
		}
	}
	return nil
}

// runDepartmentAtKey resolves key to its index within deptID's compiled
// Stages via stageNodeKey and runs from there (execution LLD §2.6):
// TargetNodeID/TargetStage and their Revert* counterparts give
// machine-addressable routing for the case a bare department reference
// can't express — a branch target that shares a department with a stage
// already run earlier in the same plan, where starting at index 0 would
// re-run it.
func (in *interpreter) runDepartmentAtKey(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, key domain.NodeKey) (domain.NodeKey, error) {
	dept := findDepartment(plan, deptID)
	if dept == nil {
		return "", fmt.Errorf("workflow: department %q not found in plan %q", deptID, plan.Name)
	}
	for i := range dept.Stages {
		if stageNodeKey(deptID, &dept.Stages[i]) == key {
			return in.runDepartmentFrom(ctx, plan, deptID, i)
		}
	}
	return "", fmt.Errorf("workflow: node %q not found in department %q stages", key, deptID)
}

func findPlan(collab *dsl.CompiledCollaboration, name string) *dsl.CompiledPlan {
	for _, p := range collab.Plans {
		if p.Name == name {
			return p
		}
	}
	return nil
}

// deptFromNodeKey extracts the department ID from a NodeKey constructed as
// "deptID/rest" (stageNodeKey in stage.go).
func deptFromNodeKey(key domain.NodeKey) string {
	s := string(key)
	if i := strings.Index(s, "/"); i >= 0 {
		return s[:i]
	}
	return s
}

// currentPendingNode returns deptID's currently in-flight task-stage node
// key, if any — a dept-prefix match over in.pending (stage.go's nodeKey ->
// resolveCh map). Empty if deptID isn't blocked on a task-stage wait right
// now (e.g. between stages, or mid message-buffer stage).
//
// Assumes one department runs directly per ParallelBranch — the same flat
// scope limit parallelDepth's own doc comment (types.go) already accepts.
func (in *interpreter) currentPendingNode(deptID string) domain.NodeKey {
	for key := range in.pending {
		if deptFromNodeKey(key) == deptID {
			return key
		}
	}
	return ""
}

func containsDept(deptIDs []string, deptID string) bool {
	for _, d := range deptIDs {
		if d == deptID {
			return true
		}
	}
	return false
}

// stageIndexAfter returns the index of the stage immediately following
// lastCompletedNode within dept, or 0 if not found — used by DEGRADED
// respawn to resume just past a failed branch's last completed stage rather
// than restarting the department from scratch.
func stageIndexAfter(dept *dsl.DepartmentDef, lastCompletedNode domain.NodeKey) int {
	for i := range dept.Stages {
		if stageNodeKey(dept.ID, &dept.Stages[i]) == lastCompletedNode {
			return i + 1
		}
	}
	return 0
}

// runExclusive evaluates and dispatches an exclusive gateway (LLD §2.6).
func (in *interpreter) runExclusive(ctx wf.Context, plan *dsl.CompiledPlan, branches []dsl.ExclusiveBranch) (stepOutcome, error) {
	winner, err := selectBranch(branches, in.lastResultJSON)
	if err != nil {
		return stepOutcome{}, err
	}
	if winner == nil {
		return stepOutcome{}, fmt.Errorf("workflow: no exclusive branch matched and no implicit else exists")
	}
	if winner.Terminates {
		return stepOutcome{Terminated: true}, nil
	}

	// Forward Target and condition-triggered RevertTo share the same
	// transfer mechanism (execution LLD §2.6 point 4); a revert additionally
	// pops history and resets the message buffer, matching force-back (§2.7).
	if winner.RevertToDept != "" {
		node, err := in.runExclusiveRevert(ctx, plan, winner)
		return stepOutcome{LastNode: node}, err
	}

	// TargetNodeID, then TargetStage, give machine-addressable routing when
	// a bare department reference would be ambiguous (LLD §2.6's field
	// table); fall back to Target's department-from-the-top otherwise.
	identifier := winner.TargetNodeID
	if identifier == "" {
		identifier = winner.TargetStage
	}
	if identifier == "" {
		node, err := in.runDepartment(ctx, plan, winner.Target)
		return stepOutcome{LastNode: node}, err
	}
	node, err := in.runDepartmentAtKey(ctx, plan, winner.Target, domain.NodeKey(winner.Target+"/"+identifier))
	return stepOutcome{LastNode: node}, err
}

// runExclusiveRevert handles a condition-triggered back-edge: pop history
// down to the revert target, reset the message buffer over the popped span,
// then dispatch — via RevertToNodeID, then RevertToStage, for precise
// routing, else RevertToDept's department-from-the-top (same precedence as
// the forward path, LLD §2.6).
func (in *interpreter) runExclusiveRevert(ctx wf.Context, plan *dsl.CompiledPlan, winner *dsl.ExclusiveBranch) (domain.NodeKey, error) {
	deptID := winner.RevertToDept
	identifier := winner.RevertToNodeID
	if identifier == "" {
		identifier = winner.RevertToStage
	}
	target := domain.NodeKey(deptID + "/" + identifier)
	popped := in.history.PopTo(target)
	in.msgBuf.ResetSpan(popped)

	if identifier == "" {
		return in.runDepartment(ctx, plan, deptID)
	}
	return in.runDepartmentAtKey(ctx, plan, deptID, target)
}
