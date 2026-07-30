package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// failedBranch is a Parallel branch whose activity exhausted retries
// non-retryably. Distinct from a paused sibling (history.go's doc comment):
// a failed branch's goroutine has already returned an error and cannot be
// resumed — only respawned as a brand-new goroutine (LLD Appendix A.2
// #8/#9/#11).
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

// runParallel dispatches ParallelBranch entries (LLD §2.5 point 2.ii): spawns
// one workflow.Go per branch and waits for every branch to settle.
// force-back arriving while branches are still active (none failed yet)
// pauses the siblings in place, regresses to the pre-fork entry inline, and
// resumes them once that regression completes (LLD §2.7's active-parallel-
// gateway extension). Only once at least one branch actually fails does the
// instance transition to DEGRADED and park for admin resolution (LLD §3.3).
func (in *interpreter) runParallel(ctx wf.Context, plan *dsl.CompiledPlan, branches []dsl.ParallelBranch, admin wf.Channel) (stepOutcome, error) {
	preFork := in.history.PreForkEntry()

	in.parallelDepth++
	defer func() { in.parallelDepth-- }()

	settle := wf.NewChannel(ctx)
	deptIDs := make([]string, 0, len(branches))
	for _, b := range branches {
		deptIDs = append(deptIDs, b.DeptID)
		wf.Go(ctx, func(gctx wf.Context) {
			out, err := in.runSteps(gctx, plan, b.Steps, admin)
			settle.Send(gctx, branchOutcome{DeptID: b.DeptID, LastNode: out.LastNode, Err: err})
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
			return stepOutcome{Terminated: true}, nil

		case SignalInstanceForceFwd:
			// Known gap, deliberately not silently dropped: force-forward
			// while a Parallel gateway is active with no failed branch yet
			// would need to abandon every still-running branch and redirect
			// past the whole gateway — that requires propagating a
			// redirect target back through every nesting level (SubWorkflow/
			// CallPool/Parallel) up to runTopLevel, which stepOutcome
			// doesn't carry today. validateSignal still allows this signal
			// here (RUNNING permits it), so log and drop rather than fail
			// the instance; a targeted force-back or waiting for a branch
			// to fail into DEGRADED (where force-forward IS implemented)
			// are the current workarounds.
			wf.GetLogger(ctx).Warn("instance-force-forward while a Parallel gateway is active with no failed branch is not implemented; dropping",
				"target_node_key", string(envelope.Signal.TargetNodeKey))

		case SignalInstanceForceBack:
			for _, d := range deptIDs {
				in.pauseDept(ctx, d)
			}
			popped := in.history.PopTo(preFork)
			in.msgBuf.ResetSpan(popped)
			// preFork is empty when this Parallel step is itself the first
			// thing the plan ever ran — there is no earlier department to
			// regress to, so treat force-back as a no-op rather than
			// looking up a nonexistent "" department.
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

// enterDegraded implements LLD §3.3's 9-step DEGRADED park/resume/respawn
// procedure. It parks the workflow function on a Selector with exactly 3
// admin-signal cases (instance-force-forward, instance-force-back,
// instance-cancel) until every failed branch resolves — respawning on
// force-back (a brand-new workflow.Go goroutine, fresh SLA timers, fresh
// message-buffer reset — never a resume), superseding on force-forward, or
// exiting the whole instance on cancel regardless of unresolved branches.
// There is no cap on repeated respawn-then-DEGRADED-again cycles.
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

		sel.AddReceive(admin, func(c wf.ReceiveChannel, more bool) {
			var envelope adminSignalEnvelope
			c.Receive(ctx, &envelope)
			sig := envelope.Signal

			switch envelope.Kind {
			case SignalInstanceCancel:
				terminated = true

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
			return stepOutcome{Terminated: true}, nil
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
