package workflow

import (
	"errors"
	"slices"
	"strconv"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

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

type gateway struct {
	own         wf.Channel
	keys        []string
	branchSteps map[string][]dsl.ExecutionStep
	resolved    map[string]bool
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
	keys := branchKeys(branches)
	cancels := make(map[string]wf.CancelFunc, len(branches))
	branchSteps := make(map[string][]dsl.ExecutionStep, len(branches))
	resolved := make(map[string]bool, len(branches))
	gw := &gateway{own: wf.NewBufferedChannel(ctx, 8), keys: keys, branchSteps: branchSteps, resolved: resolved}
	in.gateways = append(in.gateways, gw)
	defer func() { in.gateways = slices.DeleteFunc(in.gateways, func(g *gateway) bool { return g == gw }) }()
	for i, b := range branches {
		deptIDs = append(deptIDs, b.DeptID)
		key, deptID, steps := keys[i], b.DeptID, b.Steps
		branchSteps[key] = steps
		bctx, cancel := wf.WithCancel(ctx)
		cancels[key] = cancel
		wf.Go(bctx, func(gctx wf.Context) {
			out, err := in.runSteps(gctx, plan, steps, admin)
			if errors.Is(err, errStageAbandoned) {
				return
			}
			settle.Send(gctx, branchOutcome{Key: key, DeptID: deptID, LastNode: out.LastNode, Steps: steps, Err: err, Terminated: out.Terminated})
		})
	}

	var completed []completedBranch
	var failed []failedBranch
	remaining := len(branches)
	terminated := false
	for remaining > 0 && !terminated {
		sel := wf.NewSelector(ctx)
		var envelope adminSignalEnvelope
		gotAdmin := false
		receiveAdmin := func(c wf.ReceiveChannel, more bool) {
			c.Receive(ctx, &envelope)
			gotAdmin = true
		}
		sel.AddReceive(admin, receiveAdmin)
		sel.AddReceive(gw.own, receiveAdmin)
		sel.AddReceive(settle, func(c wf.ReceiveChannel, more bool) {
			var out branchOutcome
			c.Receive(ctx, &out)
			if resolved[out.Key] {
				return
			}
			if out.Terminated {
				terminated = true
				return
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
			cb, ok, err := in.handleParallelForceForward(ctx, plan, keys, branchSteps, cancels, resolved, envelope.Signal)
			if err != nil {
				return stepOutcome{}, err
			}
			if ok {
				remaining--
				completed = append(completed, cb)
			}

		case SignalInstanceForceBack:
			if err := in.handleParallelForceBack(ctx, plan, deptIDs, preFork, envelope.Signal); err != nil {
				return stepOutcome{}, err
			}
		}
	}

	if terminated {
		return stepOutcome{Terminated: true}, nil
	}
	if len(failed) == 0 {
		return stepOutcome{LastNode: preFork}, nil
	}
	return in.enterDegraded(ctx, plan, preFork, completed, failed, admin)
}

type park struct {
	ch       wf.Channel
	depts    map[string]bool
	branches []domain.FailedBranch

	respawnCancels []wf.CancelFunc
	rerunCancel    wf.CancelFunc
	rerunning      bool
}

type rerunOutcome struct{ Err error }

const parkTerminate = "park-terminate"

func (in *interpreter) openPark(ctx wf.Context, plan *dsl.CompiledPlan, failed []failedBranch) *park {
	if len(in.parks) == 0 {
		in.degradedKeepsPause = true
	}
	p := &park{ch: wf.NewBufferedChannel(ctx, 8), depts: map[string]bool{}}
	for _, fb := range failed {
		p.depts[fb.DeptID] = true
	}
	p.branches = parkedBranches(plan, failed)
	in.parks = append(in.parks, p)
	in.status = domain.InstanceStatusDegraded
	return p
}

func parkedBranches(plan *dsl.CompiledPlan, failed []failedBranch) []domain.FailedBranch {
	branches := make([]domain.FailedBranch, 0, len(failed))
	for _, fb := range failed {
		var iamDeptID string
		if dept := findDepartment(plan, fb.DeptID); dept != nil {
			iamDeptID = dept.IAMDepartmentID
		}
		branches = append(branches, domain.FailedBranch{DepartmentID: deptUUID(iamDeptID), LastNodeKey: string(fb.LastCompletedNode)})
	}
	return branches
}

func (in *interpreter) closePark(p *park) {
	in.parks = slices.DeleteFunc(in.parks, func(q *park) bool { return q == p })
	if len(in.parks) == 0 {
		in.degradedKeepsPause = false
	}
}

func (in *interpreter) writeDegraded(ctx wf.Context) error {
	var branches []domain.FailedBranch
	for _, p := range in.parks {
		branches = append(branches, p.branches...)
	}
	return updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusDegraded, FailedBranches: branches,
	})
}

func (in *interpreter) routeToParks(ctx wf.Context, envelope adminSignalEnvelope) bool {
	if len(in.parks) == 0 {
		return false
	}
	switch envelope.Kind {
	case SignalInstanceCancel:
		in.parks[0].ch.Send(ctx, envelope)
		for _, p := range in.parks[1:] {
			p.ch.Send(ctx, adminSignalEnvelope{Kind: parkTerminate})
		}
		return true
	case SignalInstanceForceFwd, SignalInstanceForceBack:
		target := envelope.Signal.TargetDeptID
		if target == "" {
			target = deptFromNodeKey(envelope.Signal.TargetNodeKey)
		}
		for _, p := range in.parks {
			if p.depts[target] {
				p.ch.Send(ctx, envelope)
				return true
			}
		}
		in.parks[len(in.parks)-1].ch.Send(ctx, envelope)
		return true
	}
	return false
}

func (in *interpreter) enterDegraded(ctx wf.Context, plan *dsl.CompiledPlan, preFork domain.NodeKey, completed []completedBranch, failed []failedBranch, admin wf.Channel) (stepOutcome, error) {
	p := in.openPark(ctx, plan, failed)
	if err := in.writeDegraded(ctx); err != nil {
		in.closePark(p)
		return stepOutcome{}, err
	}

	settle := wf.NewChannel(ctx)
	rerunDone := wf.NewChannel(ctx)
	respawning := 0

	for len(failed) > 0 || respawning > 0 || p.rerunning {
		sel := wf.NewSelector(ctx)
		var terminated bool
		var cancelErr, fatal error

		sel.AddReceive(p.ch, func(c wf.ReceiveChannel, more bool) {
			var envelope adminSignalEnvelope
			c.Receive(ctx, &envelope)
			sig := envelope.Signal

			switch envelope.Kind {
			case SignalInstanceCancel:
				terminated = true
				cancelErr = in.cancelInstanceOnSignal(ctx, sig)

			case parkTerminate:
				terminated = true

			case SignalInstanceForceFwd:
				var cb completedBranch
				var ok bool
				failed, cb, ok, fatal = in.handleDegradedForceForward(ctx, failed, sig)
				if ok {
					completed = append(completed, cb)
				}

			case SignalInstanceForceBack:
				if sig.FromDefer {
					fatal = in.startParkRerun(ctx, plan, p, preFork, rerunDone, sig)
					break
				}
				var started bool
				failed, started = in.handleDegradedForceBack(ctx, plan, p, failed, settle, admin, sig)
				if started {
					respawning++
				}
			}
		})

		sel.AddReceive(settle, func(c wf.ReceiveChannel, more bool) {
			var out branchOutcome
			c.Receive(ctx, &out)
			respawning--
			if out.Terminated {
				terminated = true
				return
			}
			if out.Err != nil {
				fb := failedBranch{DeptID: out.DeptID, Steps: out.Steps, LastCompletedNode: out.LastNode, Err: out.Err}
				failed = append(failed, fb)
				p.depts[fb.DeptID] = true
			} else {
				completed = append(completed, completedBranch{DeptID: out.DeptID, LastNode: out.LastNode})
			}
		})

		sel.AddReceive(rerunDone, func(c wf.ReceiveChannel, more bool) {
			var out rerunOutcome
			c.Receive(ctx, &out)
			p.rerunning, p.rerunCancel = false, nil
			switch {
			case errors.Is(out.Err, errInstanceTerminated):
				terminated = true
			case out.Err != nil && !errors.Is(out.Err, errPathEnded):
				fatal = out.Err
			}
		})

		sel.Select(ctx)
		if fatal != nil {
			in.closePark(p)
			return stepOutcome{}, fatal
		}
		if terminated {
			in.closePark(p)
			return stepOutcome{Terminated: true}, cancelErr
		}
		p.branches = parkedBranches(plan, failed)
	}

	if len(in.parks) > 1 {
		in.closePark(p)
		if err := in.writeDegraded(ctx); err != nil {
			return stepOutcome{}, err
		}
		return stepOutcome{LastNode: preFork}, nil
	}
	keepsPause := in.degradedKeepsPause
	if keepsPause && in.pauseHeld {
		in.closePark(p)
		in.leaveParkPaused(ctx)
		return stepOutcome{LastNode: preFork}, nil
	}
	if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusRunning,
	}); err != nil {
		in.closePark(p)
		return stepOutcome{}, err
	}
	if keepsPause && in.pauseHeld {
		in.closePark(p)
		in.leaveParkPaused(ctx)
		return stepOutcome{LastNode: preFork}, nil
	}
	in.closePark(p)
	in.pauseHeld = false
	in.status = domain.InstanceStatusRunning
	return stepOutcome{LastNode: preFork}, nil
}

func (in *interpreter) leaveParkPaused(ctx wf.Context) {
	in.degradedKeepsPause = false
	in.status = domain.InstanceStatusPaused
	if err := pauseInstance(ctx, port.PauseInstanceInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		AdminUserID: in.pauseSignal.AdminUserID, Initiator: in.pauseInitiator, RecordVersion: in.pauseSignal.RecordVersion,
	}); err != nil {
		wf.GetLogger(ctx).Warn("pauseInstance failed", "error", err)
	}
}

func (in *interpreter) handleParallelForceForward(ctx wf.Context, plan *dsl.CompiledPlan, keys []string, branchSteps map[string][]dsl.ExecutionStep, cancels map[string]wf.CancelFunc, resolved map[string]bool, sig adminSignal) (completedBranch, bool, error) {
	deptID := sig.TargetDeptID
	key := branchRunning(keys, branchSteps, resolved, deptID)
	if key == "" {
		wf.GetLogger(ctx).Warn("instance-force-forward: target_node_key doesn't name a still-active branch of this gateway; dropping",
			"target_dept_id", deptID)
		return completedBranch{}, false, nil
	}
	oldKeys := in.pendingNodesIn(branchSteps[key])
	reached := departmentEntryNode(plan, deptID, sig.TargetNodeKey)
	if len(oldKeys) > 0 {
		reached = oldKeys[0]
	}
	if err := in.recordAndRedirect(ctx, oldKeys, reached, domain.ForceRouteDirectionForward, sig); err != nil {
		if droppedRoute(ctx, err, "target_dept_id", deptID) {
			return completedBranch{}, false, nil
		}
		return completedBranch{}, false, err
	}
	if cancel, ok := cancels[key]; ok {
		cancel()
	}
	resolved[key] = true
	return completedBranch{DeptID: deptID, LastNode: sig.TargetNodeKey}, true, nil
}

// droppedRoute reports whether a failed force-route is a clean drop: only a
// refused audit write is, because it changed nothing. A refused current-node
// write comes after the audit committed and the abandoned tasks were superseded.
func droppedRoute(ctx wf.Context, err error, field, value string) bool {
	var audit *routeAuditError
	if !errors.As(err, &audit) {
		return false
	}
	wf.GetLogger(ctx).Warn("instance-force-forward: route record refused; dropping", field, value, "error", err)
	return true
}

func (in *interpreter) innermostGatewayRunning(deptID string) *gateway {
	for i := len(in.gateways) - 1; i >= 0; i-- {
		gw := in.gateways[i]
		if branchRunning(gw.keys, gw.branchSteps, gw.resolved, deptID) != "" {
			return gw
		}
	}
	return nil
}

func branchKeys(branches []dsl.ParallelBranch) []string {
	keys := make([]string, len(branches))
	for i := range branches {
		keys[i] = strconv.Itoa(i)
	}
	return keys
}

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

func (in *interpreter) handleParallelForceBack(ctx wf.Context, plan *dsl.CompiledPlan, deptIDs []string, preFork domain.NodeKey, sig adminSignal) error {
	if preFork == "" {
		wf.GetLogger(ctx).Warn("instance-force-back: no completed node to return to; dropping")
		return nil
	}
	entry := departmentEntryNode(plan, deptFromNodeKey(preFork), preFork)
	if err := in.recordAndRedirect(ctx, in.currentPendingNodes(), entry, domain.ForceRouteDirectionBack, sig); err != nil {
		return err
	}
	for _, d := range deptIDs {
		in.pauseDept(ctx, d)
	}
	popped := in.history.PopTo(preFork)
	in.msgBuf.ResetSpan(popped)
	_, _, err := in.runDepartment(ctx, plan, deptFromNodeKey(preFork))
	if errors.Is(err, errPathEnded) {
		err = nil
	}
	for _, d := range deptIDs {
		in.resumeDept(ctx, d)
	}
	return err
}

func (in *interpreter) handleDegradedForceForward(ctx wf.Context, failed []failedBranch, sig adminSignal) ([]failedBranch, completedBranch, bool, error) {
	idx := failedBranchFor(failed, sig)
	if idx < 0 {
		wf.GetLogger(ctx).Warn("instance-force-forward: target_node_key doesn't name a currently-failed branch; dropping",
			"target_node_key", sig.TargetNodeKey)
		return failed, completedBranch{}, false, nil
	}
	fb := failed[idx]
	if err := in.recordAndRedirect(ctx, []domain.NodeKey{fb.LastCompletedNode}, sig.TargetNodeKey, domain.ForceRouteDirectionForward, sig); err != nil {
		if droppedRoute(ctx, err, "target_node_key", string(sig.TargetNodeKey)) {
			return failed, completedBranch{}, false, nil
		}
		return failed, completedBranch{}, false, err
	}
	return removeFailedBranch(failed, idx), completedBranch{DeptID: fb.DeptID, LastNode: sig.TargetNodeKey}, true, nil
}

// startParkRerun answers a defer inside a park: every respawned branch is
// abandoned (it returns to the failed list), and the step before the fork runs
// again beside the park loop so the park keeps reading admin signals.
func (in *interpreter) startParkRerun(ctx wf.Context, plan *dsl.CompiledPlan, p *park, preFork domain.NodeKey, done wf.Channel, sig adminSignal) error {
	if preFork == "" {
		wf.GetLogger(ctx).Warn("stage-defer: no completed node to return to; dropping")
		return nil
	}
	for _, cancel := range p.respawnCancels {
		cancel()
	}
	p.respawnCancels = nil
	if p.rerunCancel != nil {
		p.rerunCancel()
	}
	deptID := deptFromNodeKey(preFork)
	if err := in.recordAndRedirect(ctx, in.currentPendingNodes(), departmentEntryNode(plan, deptID, preFork), domain.ForceRouteDirectionBack, sig); err != nil {
		return err
	}
	if !p.rerunning {
		in.msgBuf.ResetSpan(in.history.PopTo(preFork))
	}
	rctx, cancel := wf.WithCancel(ctx)
	p.rerunCancel, p.rerunning = cancel, true
	wf.Go(rctx, func(gctx wf.Context) {
		_, _, err := in.runDepartment(gctx, plan, deptID)
		if errors.Is(err, errStageAbandoned) {
			return
		}
		done.Send(gctx, rerunOutcome{Err: err})
	})
	return nil
}

func (in *interpreter) handleDegradedForceBack(ctx wf.Context, plan *dsl.CompiledPlan, p *park, failed []failedBranch, settle, admin wf.Channel, sig adminSignal) ([]failedBranch, bool) {
	idx := failedBranchFor(failed, sig)
	if idx < 0 {
		wf.GetLogger(ctx).Warn("instance-force-back: target_node_key doesn't name a currently-failed branch; dropping",
			"target_node_key", sig.TargetNodeKey)
		return failed, false
	}
	fb := failed[idx]
	if err := in.recordForceRouteAudit(ctx, nil, resumeNode(plan, fb), domain.ForceRouteDirectionBack, sig); err != nil {
		wf.GetLogger(ctx).Warn("instance-force-back: audit write failed; dropping", "target_node_key", sig.TargetNodeKey, "error", err)
		return failed, false
	}
	failed = removeFailedBranch(failed, idx)
	in.msgBuf.ResetSpan([]domain.NodeKey{fb.LastCompletedNode})
	bctx, cancel := wf.WithCancel(ctx)
	p.respawnCancels = append(p.respawnCancels, cancel)
	wf.Go(bctx, func(gctx wf.Context) {
		out, err := in.respawnBranch(gctx, plan, fb, admin)
		if err != nil && out.LastNode == "" {
			out.LastNode = fb.LastCompletedNode
		}
		settle.Send(gctx, branchOutcome{DeptID: fb.DeptID, LastNode: out.LastNode, Steps: fb.Steps, Err: err, Terminated: out.Terminated})
	})
	return failed, true
}

func (in *interpreter) respawnBranch(ctx wf.Context, plan *dsl.CompiledPlan, fb failedBranch, admin wf.Channel) (stepOutcome, error) {
	if fb.LastCompletedNode == "" {
		return in.runSteps(ctx, plan, fb.Steps, admin)
	}

	resumeDept := deptFromNodeKey(fb.LastCompletedNode)
	dept := findDepartment(plan, resumeDept)
	if dept == nil {
		return in.runSteps(ctx, plan, fb.Steps, admin)
	}

	node, _, err := in.runDepartmentFrom(ctx, plan, resumeDept, stageIndexAfter(dept, fb.LastCompletedNode))
	if errors.Is(err, errInstanceTerminated) {
		return stepOutcome{LastNode: node, Terminated: true}, nil
	}
	if errors.Is(err, errPathEnded) {
		return stepOutcome{LastNode: node}, nil
	}
	if err != nil {
		return stepOutcome{LastNode: node}, err
	}
	rest := stepsAfterDept(fb.Steps, resumeDept)
	if len(rest) == 0 {
		return stepOutcome{LastNode: node}, nil
	}
	out, err := in.runSteps(ctx, plan, rest, admin)
	if out.LastNode == "" {
		out.LastNode = node
	}
	return out, err
}

func stepsAfterDept(branchSteps []dsl.ExecutionStep, deptID string) []dsl.ExecutionStep {
	cont, ok := findRedirectTarget(branchSteps, deptID)
	if !ok || len(cont) == 0 {
		return nil
	}
	head := cont[0]
	if len(head.Sequential) == 0 || head.Sequential[0] != deptID {
		return cont
	}
	var out []dsl.ExecutionStep
	if rest := head.Sequential[1:]; len(rest) > 0 {
		out = append(out, dsl.ExecutionStep{Sequential: append([]string{}, rest...)})
	}
	return append(out, cont[1:]...)
}

// resumeNode is the node a respawned branch re-enters at: the stage after its
// last completed one, the first stage of the next department when that
// department is done, or the branch's first stage when nothing completed.
func resumeNode(plan *dsl.CompiledPlan, fb failedBranch) domain.NodeKey {
	steps := fb.Steps
	if fb.LastCompletedNode != "" {
		if dept := findDepartment(plan, deptFromNodeKey(fb.LastCompletedNode)); dept != nil {
			if idx := stageIndexAfter(dept, fb.LastCompletedNode); idx < len(dept.Stages) {
				return domain.NodeKey(dsl.NodeKey(dept.ID, &dept.Stages[idx]))
			}
			steps = stepsAfterDept(fb.Steps, dept.ID)
		}
	}
	if node := firstNodeOf(plan, steps); node != "" {
		return node
	}
	return fb.LastCompletedNode
}

func firstNodeOf(plan *dsl.CompiledPlan, steps []dsl.ExecutionStep) domain.NodeKey {
	for _, step := range steps {
		if len(step.Sequential) > 0 {
			return departmentEntryNode(plan, step.Sequential[0], "")
		}
	}
	return ""
}

func failedBranchFor(failed []failedBranch, sig adminSignal) int {
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
