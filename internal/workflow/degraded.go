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
	preFork     domain.NodeKey
	park        *park

	rerunDone wf.Channel
	rerunning bool
	// heldBacks are the admin force-backs that arrived while the rerun ran, in
	// arrival order; each runs after the rerun in front of it ends.
	heldBacks []adminSignal
}

// signalBuffer is the size of the admin-signal channel of a fork and of a park.
const signalBuffer = 8

// holds reports whether a task of deptID belongs to this fork: it runs in one
// of its branches, or it is the step before the fork while that step reruns.
func (g *gateway) holds(deptID string) bool {
	if g.rerunning && deptID == deptFromNodeKey(g.preFork) {
		return true
	}
	for _, key := range g.keys {
		if stepsRun(g.branchSteps[key], deptID) {
			return true
		}
	}
	return false
}

// depts lists every department the fork's branches run, nested forks included.
func (g *gateway) depts() []string {
	var out []string
	for _, key := range g.keys {
		out = appendStepDepts(out, g.branchSteps[key])
	}
	return out
}

// pausedDepts is what a rerun holds: every department the branches run except
// the step before the fork, which is the one being rerun.
func (g *gateway) pausedDepts() []string {
	preForkDept := deptFromNodeKey(g.preFork)
	return slices.DeleteFunc(g.depts(), func(d string) bool { return d == preForkDept })
}

func appendStepDepts(out []string, steps []dsl.ExecutionStep) []string {
	for _, st := range steps {
		for _, d := range st.Sequential {
			if !slices.Contains(out, d) {
				out = append(out, d)
			}
		}
		for _, b := range st.Parallel {
			out = appendStepDepts(out, b.Steps)
		}
		for _, b := range st.Exclusive {
			out = appendStepDepts(out, b.Steps)
		}
		if st.SubWorkflow != nil {
			out = appendStepDepts(out, st.SubWorkflow.Plan.Steps)
		}
	}
	return out
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
	keys := branchKeys(branches)
	cancels := make(map[string]wf.CancelFunc, len(branches))
	branchSteps := make(map[string][]dsl.ExecutionStep, len(branches))
	resolved := make(map[string]bool, len(branches))
	gw := &gateway{own: wf.NewBufferedChannel(ctx, signalBuffer), keys: keys, branchSteps: branchSteps, resolved: resolved,
		preFork: preFork, rerunDone: wf.NewChannel(ctx)}
	in.gateways = append(in.gateways, gw)
	defer func() { in.gateways = slices.DeleteFunc(in.gateways, func(g *gateway) bool { return g == gw }) }()
	defer in.releaseForkRerun(ctx, gw)
	for i, b := range branches {
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
	for (remaining > 0 || gw.rerunning) && !terminated {
		sel := wf.NewSelector(ctx)
		var envelope adminSignalEnvelope
		var rerun rerunOutcome
		gotAdmin, gotRerun, abandoned := false, false, false
		receiveAdmin := func(c wf.ReceiveChannel, more bool) {
			c.Receive(ctx, &envelope)
			gotAdmin = true
		}
		sel.AddReceive(admin, receiveAdmin)
		sel.AddReceive(gw.own, receiveAdmin)
		sel.AddReceive(gw.rerunDone, func(c wf.ReceiveChannel, more bool) {
			c.Receive(ctx, &rerun)
			gotRerun = true
		})
		sel.AddReceive(ctx.Done(), func(c wf.ReceiveChannel, more bool) { abandoned = true })
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

		if abandoned {
			return stepOutcome{}, errStageAbandoned
		}
		if gotRerun {
			if err := in.endForkRerun(ctx, plan, gw, rerun.Err); err != nil {
				return stepOutcome{}, err
			}
		}
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
			if err := in.handleParallelForceBack(ctx, plan, gw, envelope.Signal); err != nil {
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
	return in.enterDegraded(ctx, plan, gw, preFork, completed, failed, admin)
}

type park struct {
	gw       *gateway
	ch       wf.Channel
	depts    map[string]bool
	branches []domain.FailedBranch

	// ended is set once the park's loop no longer reads its channel.
	ended bool

	respawnCancels []wf.CancelFunc
	rerunCancel    wf.CancelFunc
	rerunning      bool
}

type rerunOutcome struct{ Err error }

const parkTerminate = "park-terminate"

func (in *interpreter) openPark(ctx wf.Context, plan *dsl.CompiledPlan, gw *gateway, failed []failedBranch) *park {
	if len(in.parks) == 0 {
		in.degradedKeepsPause = true
	}
	p := &park{gw: gw, ch: wf.NewBufferedChannel(ctx, signalBuffer), depts: map[string]bool{}}
	if gw != nil {
		gw.park = p
	}
	for _, fb := range failed {
		p.depts[fb.DeptID] = true
	}
	p.branches = parkedBranches(plan, failed)
	in.parks = append(in.parks, p)
	in.status = domain.InstanceStatusDegraded
	in.statusRestorePending = false
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
	p.ended = true
	if p.gw != nil && p.gw.park == p {
		p.gw.park = nil
	}
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
	if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusDegraded, FailedBranches: branches,
	}); err != nil {
		return err
	}
	in.statusRestorePending = false
	return nil
}

func (in *interpreter) liveParks() []*park {
	return slices.DeleteFunc(slices.Clone(in.parks), func(p *park) bool { return p.ended })
}

func (in *interpreter) routeToParks(ctx wf.Context, envelope adminSignalEnvelope) bool {
	parks := in.liveParks()
	if len(parks) == 0 {
		return false
	}
	switch envelope.Kind {
	case SignalInstanceCancel:
		parks[0].ch.Send(ctx, envelope)
		for _, p := range parks[1:] {
			p.ch.Send(ctx, adminSignalEnvelope{Kind: parkTerminate})
		}
		return true
	case SignalInstanceForceFwd, SignalInstanceForceBack:
		target := envelope.Signal.TargetDeptID
		if target == "" {
			target = deptFromNodeKey(envelope.Signal.TargetNodeKey)
		}
		for _, p := range parks {
			if p.depts[target] {
				p.ch.Send(ctx, envelope)
				return true
			}
		}
		parks[len(parks)-1].ch.Send(ctx, envelope)
		return true
	}
	return false
}

func (in *interpreter) enterDegraded(ctx wf.Context, plan *dsl.CompiledPlan, gw *gateway, preFork domain.NodeKey, completed []completedBranch, failed []failedBranch, admin wf.Channel) (stepOutcome, error) {
	p := in.openPark(ctx, plan, gw, failed)
	if err := in.writeDegraded(ctx); err != nil {
		in.closePark(p)
		return stepOutcome{}, err
	}

	settle := wf.NewChannel(ctx)
	rerunDone := wf.NewChannel(ctx)
	respawning := 0

	for len(failed) > 0 || respawning > 0 || p.rerunning {
		sel := wf.NewSelector(ctx)
		var terminated, abandoned bool
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

		sel.AddReceive(ctx.Done(), func(c wf.ReceiveChannel, more bool) { abandoned = true })

		sel.Select(ctx)
		if abandoned || errors.Is(fatal, errStageAbandoned) {
			return in.abandonPark(ctx, p)
		}
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

	p.ended = true
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
	in.statusRestorePending = false
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

// abandonPark closes a park whose run was cancelled from outside (an interrupting
// boundary ended the path holding the fork) and settles the instance status for
// the parks still open. Nobody is left to receive an error from this run, so a
// refused status write is logged and retried at the next step result.
func (in *interpreter) abandonPark(ctx wf.Context, p *park) (stepOutcome, error) {
	in.closePark(p)
	dctx, _ := wf.NewDisconnectedContext(ctx)
	in.settleStatusAfterAbandonedPark(dctx)
	return stepOutcome{}, errStageAbandoned
}

// settleStatusAfterAbandonedPark gives the instance the status the parks still
// open call for. Settles run one at a time and read the parks inside the lock,
// so two parks abandoned together cannot write their statuses out of order. The
// status counts in memory even when the write is refused.
func (in *interpreter) settleStatusAfterAbandonedPark(ctx wf.Context) {
	if in.settleLock == nil {
		in.settleLock = wf.NewBufferedChannel(ctx, 1)
	}
	in.settleLock.Send(ctx, struct{}{})
	defer in.settleLock.Receive(ctx, nil)
	if in.status != domain.InstanceStatusDegraded && !in.statusRestorePending {
		return
	}
	if err := in.writeStatusForParks(ctx); err != nil {
		in.statusRestorePending = true
		wf.GetLogger(ctx).Warn("restoring the instance status after an abandoned park failed; it is retried at the next step result", "error", err)
		return
	}
	in.statusRestorePending = false
}

func (in *interpreter) writeStatusForParks(ctx wf.Context) error {
	if len(in.parks) > 0 {
		in.status = domain.InstanceStatusDegraded
		return in.writeDegraded(ctx)
	}
	if in.pauseHeld {
		in.leaveParkPaused(ctx)
		return nil
	}
	in.status = domain.InstanceStatusRunning
	return updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusRunning,
	})
}

// retryStatusRestore runs again a status restore that was refused.
func (in *interpreter) retryStatusRestore(ctx wf.Context) {
	if in.statusRestorePending && !in.finishing {
		in.settleStatusAfterAbandonedPark(ctx)
	}
}

func (in *interpreter) leaveParkPaused(ctx wf.Context) {
	in.statusRestorePending = false
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
		if abandonedMidActivity(ctx) {
			return completedBranch{}, false, errStageAbandoned
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

func (in *interpreter) innermostGatewayHolding(deptID string) *gateway {
	for i := len(in.gateways) - 1; i >= 0; i-- {
		if in.gateways[i].holds(deptID) {
			return in.gateways[i]
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

// handleParallelForceBack answers a force-back, or a defer, inside a running
// fork (execution LLD §2.7): every branch saves its position at its next step,
// the step before the fork runs again beside the fork loop, and the saved
// branches resume when it ends (endForkRerun). A defer also ends the stage that
// waits on its own closed task, so that branch's department creates a new task
// when it resumes.
func (in *interpreter) handleParallelForceBack(ctx wf.Context, plan *dsl.CompiledPlan, gw *gateway, sig adminSignal) error {
	if gw.rerunning && !sig.FromDefer {
		gw.heldBacks = append(gw.heldBacks, sig)
		return nil
	}
	if !gw.rerunning {
		started, err := in.startForkRerun(ctx, plan, gw, sig, false)
		if err != nil || !started {
			return err
		}
	}
	if sig.FromDefer {
		in.endDeferredStage(sig.DeferredTaskID)
	}
	return nil
}

// startForkRerun returns the fork to the step before it. alreadyPaused says the
// saved branches are still held from the rerun before it, so they are not held twice.
func (in *interpreter) startForkRerun(ctx wf.Context, plan *dsl.CompiledPlan, gw *gateway, sig adminSignal, alreadyPaused bool) (bool, error) {
	if gw.preFork == "" {
		wf.GetLogger(ctx).Warn("instance-force-back: no completed node to return to; dropping")
		return false, nil
	}
	deptID := deptFromNodeKey(gw.preFork)
	if err := in.recordAndRedirect(ctx, in.currentPendingNodes(), departmentEntryNode(plan, deptID, gw.preFork), domain.ForceRouteDirectionBack, sig); err != nil {
		if abandonedMidActivity(ctx) {
			return false, errStageAbandoned
		}
		return false, err
	}
	if !alreadyPaused {
		for _, d := range gw.pausedDepts() {
			in.pauseDept(ctx, d)
		}
	}
	in.msgBuf.ResetSpan(in.history.PopTo(gw.preFork))
	gw.rerunning = true
	wf.Go(ctx, func(gctx wf.Context) {
		_, _, err := in.runDepartment(gctx, plan, deptID)
		gw.rerunDone.Send(gctx, rerunOutcome{Err: err})
	})
	return true, nil
}

// replayHeldBack starts the next held force-back as a rerun of its own. The
// saved branches stay held from the rerun that just ended, so none advances
// while the replay's audit is written; gw.rerunning stays true for the same
// reason, which lets releaseForkRerun open them if the replay fails.
func (in *interpreter) replayHeldBack(ctx wf.Context, plan *dsl.CompiledPlan, gw *gateway) error {
	sig := gw.heldBacks[0]
	gw.heldBacks = gw.heldBacks[1:]
	started, err := in.startForkRerun(ctx, plan, gw, sig, true)
	if err == nil && !started {
		in.releaseForkRerun(ctx, gw)
	}
	return err
}

// releaseForkRerun opens the departments a fork held for its rerun when the
// fork ends before the rerun does (cancel, abandon, terminate or a failed
// rerun). Force-backs still held are not carried out: the instance is ending,
// so they get no audit, only a warning with their count.
func (in *interpreter) releaseForkRerun(ctx wf.Context, gw *gateway) {
	if n := len(gw.heldBacks); n > 0 {
		wf.GetLogger(ctx).Warn("instance-force-back: the fork ended before its held force-backs ran; dropping held force-backs", "count", n)
		gw.heldBacks = nil
	}
	if gw.rerunning {
		gw.rerunning = false
		for _, d := range gw.pausedDepts() {
			in.resumeDept(d)
		}
	}
}

func (in *interpreter) endForkRerun(ctx wf.Context, plan *dsl.CompiledPlan, gw *gateway, err error) error {
	if (err == nil || errors.Is(err, errPathEnded)) && len(gw.heldBacks) > 0 {
		return in.replayHeldBack(ctx, plan, gw)
	}
	gw.rerunning = false
	for _, d := range gw.pausedDepts() {
		in.resumeDept(d)
	}
	if errors.Is(err, errPathEnded) {
		return nil
	}
	return err
}

// endDeferredStage ends the stage waiting on a deferred task. A defer applied
// before its stage is waiting is remembered, and the stage ends as it starts.
func (in *interpreter) endDeferredStage(taskID string) {
	if ch, ok := in.deferCh[taskID]; ok {
		ch.SendAsync(struct{}{})
		return
	}
	if !in.stageExited[taskID] {
		in.deferEarly[taskID] = true
	}
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
		if abandonedMidActivity(ctx) {
			return failed, completedBranch{}, false, errStageAbandoned
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
		if abandonedMidActivity(ctx) {
			return errStageAbandoned
		}
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
