package service

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// openDepts lists the departments of the instance's open tasks; they locate
// the parallel step the instance is in, which only the workflow itself knows
// exactly.
func (s *InstanceService) openDepts(ctx context.Context, tenantID, instanceID uuid.UUID) ([]string, error) {
	var open []string
	var cursor *port.Cursor
	for {
		page, next, err := s.Tasks.ListByInstance(ctx, tenantID, instanceID, port.PageRequest{After: cursor, Limit: 50})
		if err != nil {
			return nil, fmt.Errorf("list tasks by instance: %w", err)
		}
		for _, t := range page {
			if t.Status == domain.TaskStatusReady || t.Status == domain.TaskStatusInProgress {
				dept, _ := deptAndSuffix(t.NodeKey)
				open = append(open, dept)
			}
		}
		if next == nil {
			return open, nil
		}
		cursor = next
	}
}

func (s *InstanceService) mainPlanOf(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	compiled, err := s.fetchCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	collab, err := decodeCollaboration(compiled.CompiledPlanJSON)
	if err != nil {
		return nil, err
	}
	return findPlan(collab, collab.MainPlan), nil
}

func targetRefused(format string, args ...any) error {
	return fmt.Errorf("%w: %s", port.ErrTargetNodeNotFound, fmt.Sprintf(format, args...))
}

// validateForwardTarget refuses, before the signal, a target the workflow
// could not run: a node outside the main plan (its called plans count once
// expanded), or, in a RUNNING instance inside an open parallel step, a
// department that is not one of that step's branches (execution LLD §5.10,
// §3.3).
func (s *InstanceService) validateForwardTarget(ctx context.Context, tenantID uuid.UUID, inst *domain.Instance, targetNodeKey string) error {
	plan, err := s.mainPlanOf(ctx, tenantID, inst.WorkflowVersionID)
	if err != nil {
		return err
	}
	if plan == nil || !planHasNode(plan, targetNodeKey) {
		return port.ErrTargetNodeNotFound
	}
	if inst.Status != domain.InstanceStatusRunning {
		return nil
	}
	open, err := s.openDepts(ctx, tenantID, inst.ID)
	if err != nil {
		return err
	}
	targetDept, _ := deptAndSuffix(targetNodeKey)
	if !runsInOpenBranch(newPlanWalk(plan), open, targetDept) {
		return targetRefused("department %q is not a branch of the open parallel step", targetDept)
	}
	return nil
}

func planHasNode(plan *dsl.CompiledPlan, nodeKey string) bool {
	for i := range plan.Departments {
		dept := &plan.Departments[i]
		for j := range dept.Stages {
			if dsl.NodeKey(dept.ID, &dept.Stages[j]) == nodeKey {
				return true
			}
		}
	}
	return false
}

// runsInOpenBranch is true when no parallel step is open (the target is a
// top-level jump), or when the target's department is one of the branches of
// an open step. A step is open when one of its branches holds an open task;
// a sibling branch with no open task (a timer, an outside answer) still
// counts, because the workflow, not the task list, knows whether it is done.
func runsInOpenBranch(w *planWalk, open []string, targetDept string) bool {
	parallelOpen, found := false, false
	w.eachParallelStep(func(branches []dsl.ParallelBranch) {
		if !w.anyBranchRuns(branches, open) {
			return
		}
		parallelOpen = true
		found = found || w.anyBranchRuns(branches, []string{targetDept})
	})
	return !parallelOpen || found
}

// planWalk reads a plan the way the interpreter runs it: sequential and
// parallel departments, exclusive targets and reverts, sub-workflows, and the
// boundary paths of steps and stages.
type planWalk struct {
	plan  *dsl.CompiledPlan
	depts map[string]*dsl.DepartmentDef
}

func newPlanWalk(plan *dsl.CompiledPlan) *planWalk {
	w := &planWalk{plan: plan, depts: make(map[string]*dsl.DepartmentDef, len(plan.Departments))}
	for i := range plan.Departments {
		w.depts[plan.Departments[i].ID] = &plan.Departments[i]
	}
	return w
}

func (w *planWalk) anyBranchRuns(branches []dsl.ParallelBranch, depts []string) bool {
	return slices.ContainsFunc(branches, func(b dsl.ParallelBranch) bool {
		return slices.ContainsFunc(depts, func(d string) bool { return w.branchRuns(b, d) })
	})
}

func (w *planWalk) branchRuns(b dsl.ParallelBranch, deptID string) bool {
	return w.reaches(b.DeptID, deptID, map[string]bool{}) || w.stepsRun(b.Steps, deptID, map[string]bool{})
}

// reaches is true when department from is deptID, or leads to it through a
// boundary on one of its stages.
func (w *planWalk) reaches(from, deptID string, seen map[string]bool) bool {
	if from == "" {
		return false
	}
	if from == deptID {
		return true
	}
	if seen[from] {
		return false
	}
	seen[from] = true
	dept := w.depts[from]
	if dept == nil {
		return false
	}
	for i := range dept.Stages {
		for _, p := range stageBoundaries(&dept.Stages[i]) {
			if w.reaches(p.target, deptID, seen) || w.stepsRun(p.steps, deptID, seen) {
				return true
			}
		}
	}
	return false
}

func (w *planWalk) stepsRun(steps []dsl.ExecutionStep, deptID string, seen map[string]bool) bool {
	for i := range steps {
		if w.stepRuns(&steps[i], deptID, seen) {
			return true
		}
	}
	return false
}

func (w *planWalk) stepRuns(st *dsl.ExecutionStep, deptID string, seen map[string]bool) bool {
	for _, d := range st.Sequential {
		if w.reaches(d, deptID, seen) {
			return true
		}
	}
	for _, b := range st.Parallel {
		if w.reaches(b.DeptID, deptID, seen) || w.stepsRun(b.Steps, deptID, seen) {
			return true
		}
	}
	for _, b := range st.Exclusive {
		if w.reaches(b.Target, deptID, seen) || w.reaches(b.RevertToDept, deptID, seen) || w.stepsRun(b.Steps, deptID, seen) {
			return true
		}
	}
	if st.SubWorkflow != nil && w.stepsRun(st.SubWorkflow.Plan.Steps, deptID, seen) {
		return true
	}
	for _, p := range stepBoundaries(st) {
		if w.reaches(p.target, deptID, seen) || w.stepsRun(p.steps, deptID, seen) {
			return true
		}
	}
	return false
}

// eachParallelStep visits every parallel step the interpreter can run, in the
// main route, its branches, gateways, sub-workflows and boundary paths.
func (w *planWalk) eachParallelStep(visit func([]dsl.ParallelBranch)) {
	w.eachParallelStepIn(w.plan.Execution.Steps, visit)
	for i := range w.plan.Departments {
		for j := range w.plan.Departments[i].Stages {
			for _, p := range stageBoundaries(&w.plan.Departments[i].Stages[j]) {
				w.eachParallelStepIn(p.steps, visit)
			}
		}
	}
}

func (w *planWalk) eachParallelStepIn(steps []dsl.ExecutionStep, visit func([]dsl.ParallelBranch)) {
	for i := range steps {
		if len(steps[i].Parallel) > 0 {
			visit(steps[i].Parallel)
		}
		for _, b := range steps[i].Parallel {
			w.eachParallelStepIn(b.Steps, visit)
		}
		for _, b := range steps[i].Exclusive {
			w.eachParallelStepIn(b.Steps, visit)
		}
		if sw := steps[i].SubWorkflow; sw != nil {
			w.eachParallelStepIn(sw.Plan.Steps, visit)
		}
		for _, p := range stepBoundaries(&steps[i]) {
			w.eachParallelStepIn(p.steps, visit)
		}
	}
}

// boundaryPath is where a boundary leads: a department, a step path, or
// neither when it ends the path.
type boundaryPath struct {
	target string
	steps  []dsl.ExecutionStep
}

func stageBoundaries(st *dsl.StageDef) []boundaryPath {
	var out []boundaryPath
	if t := st.BoundaryTimer; t != nil {
		out = append(out, boundaryPath{t.TargetDept, t.Steps})
	}
	if m := st.BoundaryMessage; m != nil {
		out = append(out, boundaryPath{m.TargetDept, m.Steps})
	}
	return out
}

func stepBoundaries(st *dsl.ExecutionStep) []boundaryPath {
	var out []boundaryPath
	for _, m := range st.MessagePaths {
		out = append(out, boundaryPath{m.TargetDept, m.Steps})
	}
	if sw := st.SubWorkflow; sw != nil {
		out = append(out, pathsOf(sw.ErrorPaths, sw.TimerPaths, sw.MessagePaths)...)
	}
	if cp := st.CallPlan; cp != nil {
		out = append(out, pathsOf(cp.ErrorPaths, cp.TimerPaths, cp.MessagePaths)...)
	}
	return out
}

func pathsOf(errs []dsl.ErrorPath, timers []dsl.TimerPath, msgs []dsl.MessagePath) []boundaryPath {
	out := make([]boundaryPath, 0, len(errs)+len(timers)+len(msgs))
	for _, p := range errs {
		out = append(out, boundaryPath{p.TargetDept, p.Steps})
	}
	for _, p := range timers {
		out = append(out, boundaryPath{p.TargetDept, p.Steps})
	}
	for _, p := range msgs {
		out = append(out, boundaryPath{p.TargetDept, p.Steps})
	}
	return out
}
