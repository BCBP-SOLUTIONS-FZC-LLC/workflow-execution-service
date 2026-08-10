package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// failedBranch is a Parallel branch whose goroutine already returned a
// non-retryable error — only respawnable, never resumed (execution LLD
// Appendix A.2 #8/#9/#11).
type failedBranch struct {
	DeptID            string
	LastCompletedNode domain.NodeKey
	Err               error
}

// completedBranch is a Parallel branch that finished successfully, whether
// before, during, or after a DEGRADED window.
type completedBranch struct {
	DeptID   string
	LastNode domain.NodeKey
}

// runParallel dispatches ParallelBranch entries, one workflow.Go per branch
// (execution LLD §2.5 point 2.ii). A force-back while all branches are still
// active pauses and regresses in place (§2.7); a force-forward while still
// active supersedes and resumes past just the addressed branch, leaving
// every other sibling running untouched (§3.1's "RUNNING or DEGRADED"
// precondition on instance-force-forward, and the same "siblings are not
// cancelled" framing §3.3 already applies to DEGRADED's own force-forward
// case); DEGRADED (§3.3) only once a branch actually fails.
func (in *interpreter) runParallel(ctx wf.Context, plan *dsl.CompiledPlan, branches []dsl.ParallelBranch, admin wf.Channel) (stepOutcome, error) {
	preFork := in.history.PreForkEntry()

	in.parallelDepth++
	defer func() { in.parallelDepth-- }()

	settle := wf.NewChannel(ctx)
	deptIDs := make([]string, 0, len(branches))
	cancels := make(map[string]wf.CancelFunc, len(branches))
	// resolved guards two races a force-forward introduces: a stale settle
	// landing after its branch has already been force-forwarded past, and a
	// duplicate/late force-forward for a dept already resolved.
	resolved := make(map[string]bool, len(branches))
	for _, b := range branches {
		deptIDs = append(deptIDs, b.DeptID)
		bctx, cancel := wf.WithCancel(ctx)
		cancels[b.DeptID] = cancel
		deptID, steps := b.DeptID, b.Steps
		wf.Go(bctx, func(gctx wf.Context) {
			out, err := in.runSteps(gctx, plan, steps, admin)
			settle.Send(gctx, branchOutcome{DeptID: deptID, LastNode: out.LastNode, Err: err})
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
			if resolved[out.DeptID] {
				return // stale settle from a branch already force-forwarded past
			}
			resolved[out.DeptID] = true
			remaining--
			if out.Err != nil {
				failed = append(failed, failedBranch{DeptID: out.DeptID, LastCompletedNode: out.LastNode, Err: out.Err})
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
			sig := envelope.Signal
			deptID := sig.TargetDeptID
			if resolved[deptID] || !containsDept(deptIDs, deptID) {
				wf.GetLogger(ctx).Warn("instance-force-forward: target_dept_id doesn't name a still-active branch of this gateway; dropping",
					"target_dept_id", deptID)
				continue
			}
			var oldKeys []domain.NodeKey
			if node := in.currentPendingNode(deptID); node != "" {
				oldKeys = []domain.NodeKey{node}
			}
			// Best-effort, same reasoning as runTopLevel's and enterDegraded's
			// own ForceFwd cases: both retry unlimited on anything retryable
			// already (dbWriteActivityOptions).
			_ = recordForceRoute(ctx, port.RecordForceRouteInput{
				InstanceID: in.instanceID, TenantID: in.tenantID,
				OldNodeKeys: oldKeys, TargetNodeID: string(sig.TargetNodeKey),
				AdminUserID: sig.AdminUserID, RecordVersion: sig.RecordVersion,
			})
			_ = updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
				InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{sig.TargetNodeKey},
			})
			// Known limitation: cancel() only reliably interrupts a branch
			// mid-ExecuteActivity. A branch parked in runTaskStage's own
			// Selector waiting on a real signal (stage.go) has no ctx.Done()
			// case registered and is left orphaned/blocked — harmless, since
			// RecordForceRouteActivity above vacates its assignment, so no
			// legitimate future signal should resolve it anyway. The same
			// trade-off already exists, unflagged, in runTopLevel's own
			// force-forward case (workflow.go).
			if cancel, ok := cancels[deptID]; ok {
				cancel()
			}
			resolved[deptID] = true
			remaining--
			completed = append(completed, completedBranch{DeptID: deptID, LastNode: sig.TargetNodeKey})

		case SignalInstanceForceBack:
			for _, d := range deptIDs {
				in.pauseDept(ctx, d)
			}
			popped := in.history.PopTo(preFork)
			in.msgBuf.ResetSpan(popped)
			// preFork empty means this Parallel step ran first; no-op rather
			// than looking up a nonexistent "" department.
			var err error
			if preFork != "" {
				_, err = in.runDepartment(ctx, plan, deptFromNodeKey(preFork))
			}
			for _, d := range deptIDs {
				in.resumeDept(ctx, d)
			}
			if err != nil {
				return stepOutcome{}, err
			}
		}
	}

	if len(failed) == 0 {
		return stepOutcome{LastNode: preFork}, nil
	}
	return in.enterDegraded(ctx, plan, preFork, completed, failed, admin)
}

// enterDegraded implements the DEGRADED park/resume/respawn procedure
// (execution LLD §3.3): parks on a Selector with 3 admin-signal cases until
// every failed branch resolves, respawning on force-back, superseding on
// force-forward, or exiting on cancel. No cap on repeated respawn cycles.
func (in *interpreter) enterDegraded(ctx wf.Context, plan *dsl.CompiledPlan, preFork domain.NodeKey, completed []completedBranch, failed []failedBranch, admin wf.Channel) (stepOutcome, error) {
	in.status = domain.InstanceStatusDegraded
	if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusDegraded,
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
				idx := indexOfFailedBranch(failed, sig.TargetDeptID)
				if idx < 0 {
					return
				}
				fb := failed[idx]
				// Best-effort, same reasoning as runTopLevel's ForceFwd case
				// (workflow.go): both retry unlimited on anything retryable.
				_ = recordForceRoute(ctx, port.RecordForceRouteInput{
					InstanceID: in.instanceID, TenantID: in.tenantID,
					OldNodeKeys: []domain.NodeKey{fb.LastCompletedNode}, TargetNodeID: string(sig.TargetNodeKey),
					AdminUserID: sig.AdminUserID, RecordVersion: sig.RecordVersion,
				})
				_ = updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
					InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{sig.TargetNodeKey},
				})
				completed = append(completed, completedBranch{DeptID: fb.DeptID, LastNode: sig.TargetNodeKey})
				failed = removeFailedBranch(failed, idx)

			case SignalInstanceForceBack:
				idx := indexOfFailedBranch(failed, sig.TargetDeptID)
				if idx < 0 {
					return
				}
				fb := failed[idx]
				failed = removeFailedBranch(failed, idx)
				in.msgBuf.ResetSpan([]domain.NodeKey{fb.LastCompletedNode})
				respawning++
				wf.Go(ctx, func(gctx wf.Context) {
					dept := findDepartment(plan, fb.DeptID)
					startIdx := 0
					if dept != nil {
						startIdx = stageIndexAfter(dept, fb.LastCompletedNode)
					}
					node, err := in.runDepartmentFrom(gctx, plan, fb.DeptID, startIdx)
					settle.Send(gctx, branchOutcome{DeptID: fb.DeptID, LastNode: node, Err: err})
				})
			}
		})

		sel.AddReceive(settle, func(c wf.ReceiveChannel, more bool) {
			var out branchOutcome
			c.Receive(ctx, &out)
			respawning--
			if out.Err != nil {
				// Respawn failed again — LLD §3.3 point 9: no cap on
				// repeated respawn attempts, the instance simply parks in
				// DEGRADED again.
				failed = append(failed, failedBranch{DeptID: out.DeptID, LastCompletedNode: out.LastNode, Err: out.Err})
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
