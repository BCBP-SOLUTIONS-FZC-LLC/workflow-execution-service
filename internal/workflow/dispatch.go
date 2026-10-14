package workflow

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func (in *interpreter) runSteps(ctx wf.Context, plan *dsl.CompiledPlan, steps []dsl.ExecutionStep, admin wf.Channel) (stepOutcome, error) {
	out, err := in.runStepList(ctx, plan, steps, admin)
	if errors.Is(err, errPathEnded) {
		return out, nil
	}
	return out, err
}

func (in *interpreter) runStepList(ctx wf.Context, plan *dsl.CompiledPlan, steps []dsl.ExecutionStep, admin wf.Channel) (stepOutcome, error) {
	var last domain.NodeKey
	var lastResult string
	for i := range steps {
		step := &steps[i]

		if step.IOMapping != nil {
			ctxJSON, err := applyIOMapping(in.contextJSON, step.IOMapping, getVersion(ctx, ioInputRuleChangeID) != wf.DefaultVersion)
			if err != nil {
				return stepOutcome{LastNode: last, LastResult: lastResult}, err
			}
			in.contextJSON = ctxJSON
		}

		switch {
		case len(step.Sequential) > 0:
			for _, deptID := range step.Sequential {
				node, result, err := in.runDepartment(ctx, plan, deptID)
				if err != nil {
					return stepOutcome{LastNode: last, LastResult: lastResult}, err
				}
				last = node
				if result != "" {
					lastResult = result
				}
			}

		case len(step.Parallel) > 0:
			out, err := in.runParallel(ctx, plan, step.Parallel, admin)
			last = out.LastNode
			if err != nil || out.Terminated {
				return out, err
			}

		case len(step.Exclusive) > 0:
			out, err := in.runExclusive(ctx, plan, step.Exclusive, lastResult, admin)
			last = out.LastNode
			if out.LastResult != "" {
				lastResult = out.LastResult
			}
			if err != nil || out.Terminated {
				return out, err
			}

		case step.SubWorkflow != nil:
			node, err := in.runSubWorkflow(ctx, plan, step.SubWorkflow, admin)
			last = node
			if err != nil {
				return stepOutcome{LastNode: last, LastResult: lastResult}, err
			}

		default:
			return stepOutcome{LastNode: last, LastResult: lastResult}, fmt.Errorf("workflow: execution step has no populated variant")
		}
	}
	return stepOutcome{LastNode: last, LastResult: lastResult}, nil
}

func (in *interpreter) runDepartment(ctx wf.Context, plan *dsl.CompiledPlan, deptID string) (domain.NodeKey, string, error) {
	return in.runDepartmentFrom(ctx, plan, deptID, 0)
}

func (in *interpreter) spawnNonInterruptingTarget(ctx wf.Context, plan *dsl.CompiledPlan, targetDept string) {
	wf.Go(ctx, func(gctx wf.Context) {
		_, _, _ = in.runDepartment(gctx, plan, targetDept)
	})
}

func (in *interpreter) runDepartmentFrom(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, startIdx int) (domain.NodeKey, string, error) {
	dept := findDepartment(plan, deptID)
	if dept == nil {
		return "", "", fmt.Errorf("workflow: department %q not found in plan %q", deptID, plan.Name)
	}
	var last domain.NodeKey
	var lastResult string
	for i := startIdx; i < len(dept.Stages); i++ {
		in.checkPaused(ctx, deptID)
		node, result, err := in.runStage(ctx, plan, deptID, &dept.Stages[i])
		if err != nil {
			return last, lastResult, err
		}
		last = node
		if result != "" {
			lastResult = result
		}
	}
	return last, lastResult, nil
}

func findDepartment(plan *dsl.CompiledPlan, id string) *dsl.DepartmentDef {
	for i := range plan.Departments {
		if plan.Departments[i].ID == id {
			return &plan.Departments[i]
		}
	}
	return nil
}

func (in *interpreter) runDepartmentAtKey(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, key domain.NodeKey) (domain.NodeKey, string, error) {
	dept := findDepartment(plan, deptID)
	if dept == nil {
		return "", "", fmt.Errorf("workflow: department %q not found in plan %q", deptID, plan.Name)
	}
	for i := range dept.Stages {
		if domain.NodeKey(dsl.NodeKey(deptID, &dept.Stages[i])) == key {
			return in.runDepartmentFrom(ctx, plan, deptID, i)
		}
	}
	return "", "", fmt.Errorf("workflow: node %q not found in department %q stages", key, deptID)
}

func findPlan(collab *dsl.CompiledCollaboration, name string) *dsl.CompiledPlan {
	for _, p := range collab.Plans {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func deptFromNodeKey(key domain.NodeKey) string {
	s := string(key)
	if i := strings.Index(s, "/"); i >= 0 {
		return s[:i]
	}
	return s
}

func deptUUID(iamDeptID string) uuid.UUID {
	id, _ := uuid.Parse(iamDeptID)
	return id
}

func (in *interpreter) currentPendingNode(deptID string) domain.NodeKey {
	for key := range in.pending {
		if deptFromNodeKey(key.Node) == deptID {
			return key.Node
		}
	}
	return ""
}

func (in *interpreter) currentPendingNodes() []domain.NodeKey {
	keys := make([]domain.NodeKey, 0, len(in.pending))
	for key := range in.pending {
		keys = append(keys, key.Node)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func stageIndexAfter(dept *dsl.DepartmentDef, lastCompletedNode domain.NodeKey) int {
	for i := range dept.Stages {
		if domain.NodeKey(dsl.NodeKey(dept.ID, &dept.Stages[i])) == lastCompletedNode {
			return i + 1
		}
	}
	return 0
}

const maxRevertCycles = 50

func (in *interpreter) settleReverts(
	ctx wf.Context, plan *dsl.CompiledPlan, branches []dsl.ExclusiveBranch, resultJSON string,
) (winner *dsl.ExclusiveBranch, outcome stepOutcome, done bool, err error) {
	var lastNode domain.NodeKey
	for cycle := 0; ; cycle++ {
		winner, err = selectBranch(branches, resultJSON)
		if err != nil {
			return nil, stepOutcome{}, true, err
		}
		if winner == nil {
			if cycle == 0 {
				return nil, stepOutcome{}, true, fmt.Errorf("workflow: no exclusive branch matched and no implicit else exists")
			}
			return nil, stepOutcome{LastNode: lastNode, LastResult: resultJSON}, true, nil
		}
		if winner.RevertToDept == "" {
			return winner, stepOutcome{LastNode: lastNode, LastResult: resultJSON}, false, nil
		}
		if cycle >= maxRevertCycles {
			return nil, stepOutcome{LastNode: lastNode, LastResult: resultJSON}, true, fmt.Errorf(
				"workflow: exclusive gateway reverted to %q more than %d times; failing closed rather than taking the forward branch the condition did not select",
				winner.RevertToDept, maxRevertCycles)
		}

		node, result, rerr := in.runExclusiveRevert(ctx, plan, winner)
		if rerr != nil {
			return nil, stepOutcome{LastNode: node, LastResult: result}, true, rerr
		}
		lastNode, resultJSON = node, result
	}
}

func (in *interpreter) runExclusive(ctx wf.Context, plan *dsl.CompiledPlan, branches []dsl.ExclusiveBranch, resultJSON string, admin wf.Channel) (stepOutcome, error) {
	winner, outcome, done, err := in.settleReverts(ctx, plan, branches, resultJSON)
	if done || err != nil {
		return outcome, err
	}

	if len(winner.Steps) > 0 {
		out, err := in.runStepList(ctx, plan, winner.Steps, admin)
		if err != nil || out.Terminated || !winner.Terminates {
			return out, err
		}
		return out, errPathEnded
	}
	if winner.Terminates {
		if getVersion(ctx, exclusiveEndCompletesPathChangeID) == wf.DefaultVersion {
			return stepOutcome{Terminated: true}, nil
		}
		return outcome, errPathEnded
	}

	if winner.Target == "" {
		return outcome, nil
	}

	identifier := winner.TargetNodeID
	if identifier == "" {
		identifier = winner.TargetStage
	}
	if identifier == "" {
		node, result, err := in.runDepartment(ctx, plan, winner.Target)
		return stepOutcome{LastNode: node, LastResult: result}, err
	}
	node, result, err := in.runDepartmentAtKey(ctx, plan, winner.Target, domain.NodeKey(winner.Target+"/"+identifier))
	return stepOutcome{LastNode: node, LastResult: result}, err
}

func (in *interpreter) runExclusiveRevert(ctx wf.Context, plan *dsl.CompiledPlan, winner *dsl.ExclusiveBranch) (domain.NodeKey, string, error) {
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
