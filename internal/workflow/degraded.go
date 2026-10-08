package workflow

import (
	"errors"
	"slices"
	"strconv"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// failedBranch is a Parallel branch whose goroutine already returned a
// non-retryable error — only respawnable, never resumed (execution LLD
// Appendix A.2 #8/#9/#11). Steps is the branch's own original dispatch (LLD
// §2.5 point 2.ii) — needed for respawn to correctly resume a branch whose
// Steps ran more than a single bare department (respawnBranch below).
type failedBranch struct {
	DeptID            string
	Steps             []dsl.ExecutionStep
	LastCompletedNode domain.NodeKey
	Err               error
}

type completedBranch struct {
	DeptID   string
	LastNode domain.NodeKey
}

func (in *interpreter) runParallel(ctx wf.Context, plan *dsl.CompiledPlan, branches []dsl.ParallelBranch, admin wf.Channel) (stepOutcome, error) {
	preFork := in.history.PreForkEntry()

	in.parallelDepth++
	in.forkEntries = append(in.forkEntries, preFork)
	defer func() {
		in.parallelDepth--
		in.forkEntries = in.forkEntries[:len(in.forkEntries)-1]
	}()

	settle := wf.NewChannel(ctx)
	deptIDs := make([]string, 0, len(branches))
	keys := branchKeys(ctx, branches)
	cancels := make(map[string]wf.CancelFunc, len(branches))
	branchSteps := make(map[string][]dsl.ExecutionStep, len(branches))
	// resolved guards two races a force-forward introduces: a stale settle
	// landing after its branch has already been force-forwarded past, and a
	// duplicate/late force-forward for a branch already resolved.
	resolved := make(map[string]bool, len(branches))
	for i, b := range branches {
		deptIDs = append(deptIDs, b.DeptID)
		key, deptID, steps := keys[i], b.DeptID, b.Steps
		branchSteps[key] = steps
		bctx, cancel := wf.WithCancel(ctx)
		cancels[key] = cancel
		wf.Go(bctx, func(gctx wf.Context) {
			out, err := in.runSteps(gctx, plan, steps, admin)
			if errors.Is(err, errStageAbandoned) {
				return // force-forwarded away; settle has no listener left for this branch
			}
			settle.Send(gctx, branchOutcome{Key: key, DeptID: deptID, LastNode: out.LastNode, Steps: steps, Err: err})
		})
	}

	var completed []completedBranch
	var failed []failedBranch
	remaining := len(branches)
	for remaining > 0 {
		sel := wf.NewSelector(ctx)
		var envelope adminSignalEnvelope
		gotAdmin := false
		sel.AddReceive(admin, func(c wf.ReceiveChannel, more bool) {
			c.Receive(ctx, &envelope)
			gotAdmin = true
		})
		sel.AddReceive(settle, func(c wf.ReceiveChannel, more bool) {
			var out branchOutcome
			c.Receive(ctx, &out)
			if resolved[out.Key] {
				return // stale settle from a branch already force-forwarded past
			}
			resolved[out.Key] = true
			remaining--
			if out.Err != nil {
				failed = append(failed, failedBranch{DeptID: out.DeptID, Steps: branchSteps[out.Key], LastCompletedNode: out.LastNode, Err: out.Err})
			} else {
				completed = append(completed, completedBranch{DeptID: out.DeptID, LastNode: out.LastNode})
			}
		})
		sel.Select(ctx)

		if !gotAdmin {
			continue
		}
		switch envelope.Kind {
		case SignalInstanceCancel:
			return stepOutcome{Terminated: true}, in.cancelInstanceOnSignal(ctx, envelope.Signal)

		case SignalInstanceForceFwd:
			if cb, ok := in.handleParallelForceForward(ctx, keys, branchSteps, cancels, resolved, envelope.Signal); ok {
				remaining--
				completed = append(completed, cb)
			}

		case SignalInstanceForceBack:
			if err := in.handleParallelForceBack(ctx, plan, deptIDs, preFork); err != nil {
				return stepOutcome{}, err
			}
		}
	}

	if len(failed) == 0 {
		return stepOutcome{LastNode: preFork}, nil
	}
	return in.enterDegraded(ctx, plan, preFork, completed, failed, admin)
}

func (in *interpreter) enterDegraded(ctx wf.Context, plan *dsl.CompiledPlan, preFork domain.NodeKey, completed []completedBranch, failed []failedBranch, admin wf.Channel) (stepOutcome, error) {
	in.status = domain.InstanceStatusDegraded
	failedBranches := make([]domain.FailedBranch, len(failed))
	for i, fb := range failed {
		var iamDeptID string
		if dept := findDepartment(plan, fb.DeptID); dept != nil {
			iamDeptID = dept.IAMDepartmentID
		}
		failedBranches[i] = domain.FailedBranch{DepartmentID: deptUUID(iamDeptID), LastNodeKey: string(fb.LastCompletedNode)}
	}
	if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusDegraded,
		FailedBranches: failedBranches,
	}); err != nil {
		return stepOutcome{}, err
	}

	settle := wf.NewChannel(ctx)
	respawning := 0

	for len(failed) > 0 || respawning > 0 {
		sel := wf.NewSelector(ctx)
		var terminated bool
		var cancelErr error

		sel.AddReceive(admin, func(c wf.ReceiveChannel, more bool) {
			var envelope adminSignalEnvelope
			c.Receive(ctx, &envelope)
			sig := envelope.Signal

			switch envelope.Kind {
			case SignalInstanceCancel:
				terminated = true
				cancelErr = in.cancelInstanceOnSignal(ctx, sig)

			case SignalInstanceForceFwd:
				var cb completedBranch
				var ok bool
				failed, cb, ok = in.handleDegradedForceForward(ctx, failed, sig)
				if ok {
					completed = append(completed, cb)
				}

			case SignalInstanceForceBack:
				var started bool
				failed, started = in.handleDegradedForceBack(ctx, plan, failed, settle, admin, sig)
				if started {
					respawning++
				}
			}
		})

		sel.AddReceive(settle, func(c wf.ReceiveChannel, more bool) {
			var out branchOutcome
			c.Receive(ctx, &out)
			respawning--
			if out.Err != nil {
				fb := failedBranch{DeptID: out.DeptID, LastCompletedNode: out.LastNode, Err: out.Err}
				if getVersion(ctx, forceRouteByNodeKeyChangeID) != wf.DefaultVersion {
					fb.Steps = out.Steps
				}
				failed = append(failed, fb)
			} else {
				completed = append(completed, completedBranch{DeptID: out.DeptID, LastNode: out.LastNode})
			}
		})

		sel.Select(ctx)
		if terminated {
			return stepOutcome{Terminated: true}, cancelErr
		}
	}

	in.status = domain.InstanceStatusRunning
	if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusRunning,
	}); err != nil {
		return stepOutcome{}, err
	}
	return stepOutcome{LastNode: preFork}, nil
}

func (in *interpreter) handleParallelForceForward(ctx wf.Context, keys []string, branchSteps map[string][]dsl.ExecutionStep, cancels map[string]wf.CancelFunc, resolved map[string]bool, sig adminSignal) (completedBranch, bool) {
	deptID := sig.TargetDeptID
	key := branchRunning(keys, branchSteps, resolved, deptID)
	if key == "" {
		wf.GetLogger(ctx).Warn("instance-force-forward: target_node_key doesn't name a still-active branch of this gateway; dropping",
			"target_dept_id", deptID)
		return completedBranch{}, false
	}
	var oldKeys []domain.NodeKey
	if node := in.currentPendingNode(deptID); node != "" {
		oldKeys = []domain.NodeKey{node}
	}
	in.recordAndRedirect(ctx, oldKeys, sig.TargetNodeKey, domain.ForceRouteDirectionForward, sig)
	if cancel, ok := cancels[key]; ok {
		cancel()
	}
	resolved[key] = true
	return completedBranch{DeptID: deptID, LastNode: sig.TargetNodeKey}, true
}

// branchKeys names each branch of a Parallel step for the join: by its place
// in the step, or, in a history recorded before parallelJoinByBranchChangeID,
// by its DeptID.
func branchKeys(ctx wf.Context, branches []dsl.ParallelBranch) []string {
	byPlace := getVersion(ctx, parallelJoinByBranchChangeID) != wf.DefaultVersion
	keys := make([]string, len(branches))
	for i, b := range branches {
		keys[i] = b.DeptID
		if byPlace {
			keys[i] = strconv.Itoa(i)
		}
	}
	return keys
}

// branchRunning returns the key of the unresolved branch that runs
// department deptID: the branch keyed by it, or one whose steps name it.
func branchRunning(keys []string, branchSteps map[string][]dsl.ExecutionStep, resolved map[string]bool, deptID string) string {
	for _, key := range keys {
		if !resolved[key] && key == deptID {
			return key
		}
	}
	for _, key := range keys {
		if !resolved[key] && key != deptID && stepsRun(branchSteps[key], deptID) {
			return key
		}
	}
	return ""
}

// stepsRun reports whether steps run department deptID anywhere inside them.
func stepsRun(steps []dsl.ExecutionStep, deptID string) bool {
	for _, st := range steps {
		if slices.Contains(st.Sequential, deptID) {
			return true
		}
		for _, b := range st.Parallel {
			if stepsRun(b.Steps, deptID) {
				return true
			}
		}
		for _, b := range st.Exclusive {
			if stepsRun(b.Steps, deptID) {
				return true
			}
		}
		if st.SubWorkflow != nil && stepsRun(st.SubWorkflow.Plan.Steps, deptID) {
			return true
		}
	}
	return false
}

func (in *interpreter) handleParallelForceBack(ctx wf.Context, plan *dsl.CompiledPlan, deptIDs []string, preFork domain.NodeKey) error {
	for _, d := range deptIDs {
		in.pauseDept(ctx, d)
	}
	popped := in.history.PopTo(preFork)
	in.msgBuf.ResetSpan(popped)
	var err error
	if preFork != "" {
		_, _, err = in.runDepartment(ctx, plan, deptFromNodeKey(preFork))
	}
	// The fork has already happened: a boundary ending the re-run's path
	// leaves the branches to resume and the join to proceed.
	if errors.Is(err, errPathEnded) {
		err = nil
	}
	for _, d := range deptIDs {
		in.resumeDept(ctx, d)
	}
	return err
}

func (in *interpreter) handleDegradedForceForward(ctx wf.Context, failed []failedBranch, sig adminSignal) ([]failedBranch, completedBranch, bool) {
	idx := in.failedBranchFor(ctx, failed, sig)
	if idx < 0 {
		wf.GetLogger(ctx).Warn("instance-force-forward: target_node_key doesn't name a currently-failed branch; dropping",
			"target_node_key", sig.TargetNodeKey)
		return failed, completedBranch{}, false
	}
	fb := failed[idx]
	in.recordAndRedirect(ctx, []domain.NodeKey{fb.LastCompletedNode}, sig.TargetNodeKey, domain.ForceRouteDirectionForward, sig)
	return removeFailedBranch(failed, idx), completedBranch{DeptID: fb.DeptID, LastNode: sig.TargetNodeKey}, true
}

func (in *interpreter) handleDegradedForceBack(ctx wf.Context, plan *dsl.CompiledPlan, failed []failedBranch, settle, admin wf.Channel, sig adminSignal) ([]failedBranch, bool) {
	idx := in.failedBranchFor(ctx, failed, sig)
	if idx < 0 {
		wf.GetLogger(ctx).Warn("instance-force-back: target_node_key doesn't name a currently-failed branch; dropping",
			"target_node_key", sig.TargetNodeKey)
		return failed, false
	}
	fb := failed[idx]
	failed = removeFailedBranch(failed, idx)
	in.recordForceRouteAudit(ctx, nil, fb.LastCompletedNode, domain.ForceRouteDirectionBack, sig)
	in.msgBuf.ResetSpan([]domain.NodeKey{fb.LastCompletedNode})
	wf.Go(ctx, func(gctx wf.Context) {
		node, err := in.respawnBranch(gctx, plan, fb, admin)
		if err != nil && node == "" && getVersion(gctx, forceRouteByNodeKeyChangeID) != wf.DefaultVersion {
			node = fb.LastCompletedNode // a branch that fails again has still completed what it had
		}
		settle.Send(gctx, branchOutcome{DeptID: fb.DeptID, LastNode: node, Steps: fb.Steps, Err: err})
	})
	return failed, true
}

// respawnBranch resumes a DEGRADED-failed branch (execution LLD §3.3's
// respawn procedure). fb.LastCompletedNode's own department is what
// actually needs resuming — not necessarily fb.DeptID itself, since a
// branch's Steps can run more than one department in sequence before
// failing (fb.DeptID is only the branch's own stable identity, set once at
// dispatch — dispatch.go's runParallel).
func (in *interpreter) respawnBranch(ctx wf.Context, plan *dsl.CompiledPlan, fb failedBranch, admin wf.Channel) (domain.NodeKey, error) {
	if fb.LastCompletedNode == "" {
		// Nothing in this branch completed before it failed — safe to
		// re-run its entire original step list from the top.
		out, err := in.runSteps(ctx, plan, fb.Steps, admin)
		return out.LastNode, err
	}

	resumeDept := deptFromNodeKey(fb.LastCompletedNode)
	dept := findDepartment(plan, resumeDept)
	if dept == nil {
		// resumeDept isn't a plain department (e.g. inside a SubWorkflow's
		// own inline recursion) — this interpreter has no
		// finer-grained resume pointer for that shape. Re-running the whole
		// branch is the safe fallback, matching redirectSteps' own
		// documented isolation fallback for the same structural limit
		// (workflow.go) — it may re-run an already-completed department
		// within this branch, but that beats respawn failing every time.
		out, err := in.runSteps(ctx, plan, fb.Steps, admin)
		return out.LastNode, err
	}

	node, _, err := in.runDepartmentFrom(ctx, plan, resumeDept, stageIndexAfter(dept, fb.LastCompletedNode))
	// A guard: LastCompletedNode only ever names a completed department's
	// last stage today, so this resume runs no stage that could end a path.
	if errors.Is(err, errPathEnded) {
		return node, nil
	}
	if err != nil {
		return node, err
	}
	rest := stepsAfterDept(fb.Steps, resumeDept)
	if len(rest) == 0 {
		return node, nil
	}
	out, err := in.runSteps(ctx, plan, rest, admin)
	if out.LastNode != "" {
		node = out.LastNode
	}
	return node, err
}

// stepsAfterDept returns whatever branchSteps still queued strictly after
// deptID within its own top-level Sequential list — deptID itself is
// excluded, since respawnBranch above already dispatches it separately via
// a stage-indexed resume. deptID nested inside a Parallel/Exclusive/
// SubWorkflow step returns nil: this interpreter has no finer-grained
// continuation pointer for that shape (same limit as redirectSteps'
// isolation fallback, workflow.go).
func stepsAfterDept(branchSteps []dsl.ExecutionStep, deptID string) []dsl.ExecutionStep {
	for i, step := range branchSteps {
		for j, d := range step.Sequential {
			if d == deptID {
				var out []dsl.ExecutionStep
				if rest := step.Sequential[j+1:]; len(rest) > 0 {
					out = append(out, dsl.ExecutionStep{Sequential: append([]string{}, rest...)})
				}
				return append(out, branchSteps[i+1:]...)
			}
		}
	}
	return nil
}

// failedBranchFor finds the failed branch a signal applies to. Before
// forceRouteByNodeKeyChangeID only a branch keyed by the signal's department
// matched. Now the branch is also the one whose steps run that department, or
// that last completed a node of it, and a lone failed branch needs no name.
func (in *interpreter) failedBranchFor(ctx wf.Context, failed []failedBranch, sig adminSignal) int {
	if getVersion(ctx, forceRouteByNodeKeyChangeID) == wf.DefaultVersion {
		return indexOfFailedBranch(failed, sig.TargetDeptID)
	}
	if sig.TargetDeptID == "" && len(failed) == 1 {
		return 0
	}
	if idx := indexOfFailedBranch(failed, sig.TargetDeptID); idx >= 0 {
		return idx
	}
	for i, fb := range failed {
		if stepsRun(fb.Steps, sig.TargetDeptID) || deptFromNodeKey(fb.LastCompletedNode) == sig.TargetDeptID {
			return i
		}
	}
	return -1
}

func indexOfFailedBranch(failed []failedBranch, deptID string) int {
	for i, fb := range failed {
		if fb.DeptID == deptID {
			return i
		}
	}
	return -1
}

func removeFailedBranch(failed []failedBranch, idx int) []failedBranch {
	return append(failed[:idx:idx], failed[idx+1:]...)
}
