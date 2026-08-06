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
	deptID := winner.Target
	if winner.RevertToDept != "" {
		deptID = winner.RevertToDept
		target := domain.NodeKey(deptID + "/" + winner.RevertToStage)
		popped := in.history.PopTo(target)
		in.msgBuf.ResetSpan(popped)
	}
	node, err := in.runDepartment(ctx, plan, deptID)
	return stepOutcome{LastNode: node}, err
}
