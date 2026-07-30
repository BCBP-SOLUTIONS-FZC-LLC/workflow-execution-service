package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// Signal name prefixes, per LLD §3.1's signal catalogue. The actual channel
// name is prefix + ":" + instanceID (or + ":" + taskID for task-scoped
// signals, not handled by this instance-scoped router).
const (
	SignalStageTransition   = "stage-transition"
	SignalStageDefer        = "stage-defer"
	SignalInstancePause     = "instance-pause"
	SignalInstanceResume    = "instance-resume"
	SignalInstanceCancel    = "instance-cancel"
	SignalInstanceForceFwd  = "instance-force-forward"
	SignalInstanceForceBack = "instance-force-back"
)

// signalPreconditions is LLD §3.1's signal table, reduced to the
// instance-status precondition each signal requires. This is deliberately
// the ONLY gate a signal passes through before it may reach any Selector —
// an instance-pause signal delivered while DEGRADED is rejected here, before
// the DEGRADED park loop's Selector (which registers no case for it at all)
// ever sees it (LLD §7.2 test #5).
// SignalStageTransition/SignalStageDefer allow DEGRADED too: DEGRADED parks
// only the failed branches (LLD §3.3) — a respawned branch's own task, or
// any branch that never failed, still resolves via ordinary stage-transition
// signals during that window.
var signalPreconditions = map[string][]domain.InstanceStatus{
	SignalStageTransition:   {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalStageDefer:        {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalInstancePause:     {domain.InstanceStatusRunning},
	SignalInstanceResume:    {domain.InstanceStatusPaused},
	SignalInstanceCancel:    {domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded},
	SignalInstanceForceFwd:  {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalInstanceForceBack: {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
}

// validateSignal is the pure precondition gate: does signal make sense given
// the instance's current status? Table-driven tested without any Temporal
// environment (test/workflow/signals_test.go).
func validateSignal(status domain.InstanceStatus, signal string) error {
	allowed, ok := signalPreconditions[signal]
	if !ok {
		return fmt.Errorf("workflow: unknown signal %q", signal)
	}
	for _, s := range allowed {
		if s == status {
			return nil
		}
	}
	return fmt.Errorf("workflow: signal %q rejected: instance status is %s, requires one of %v", signal, status, allowed)
}

// stageTransitionSignal is the payload of SignalStageTransition (LLD §3.1).
type stageTransitionSignal struct {
	DeptID        string
	ToStage       string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
}

// stageDeferSignal is the payload of SignalStageDefer (LLD §3.1).
type stageDeferSignal struct {
	DeptID        string
	FromStage     string
	Reason        string
	UserID        string
	RecordVersion int64
}

// adminSignal is the shared payload shape for every instance-level admin
// signal (pause/resume/cancel/force-forward/force-back).
//
// TargetDeptID is not part of the LLD's literal §3.1 payload table — it is
// added here to disambiguate which failed branch a force-forward/force-back
// applies to when DEGRADED has more than one failed branch at once. The LLD
// excerpt available to this task doesn't specify how that disambiguation is
// carried on the wire (point 8 only says "each signal addresses exactly one
// failed branch"), so this is a documented, smallest-reasonable-assumption
// extension pending confirmation from whichever task owns the real signal
// payload contract.
type adminSignal struct {
	AdminUserID   string
	Reason        string
	TargetDeptID  string
	TargetNodeKey domain.NodeKey // set only for instance-force-forward
	RecordVersion int64
}

// runSignalRouter is the always-on background goroutine (started once, in
// Execute) draining every raw signal channel for this instance: it validates
// each against validateSignal before anything else can react to it — a
// signal invalid for the current status is logged and dropped here, never
// reaching a downstream Selector (LLD §7.2 test #5) — then dispatches valid
// ones. stage-transition forwards to whichever runTaskStage call is waiting
// on the addressed node; admin signals route to admin or baseAdmin per
// in.parallelDepth (see its doc comment in types.go).
func (in *interpreter) runSignalRouter(ctx wf.Context, admin, baseAdmin wf.Channel) {
	logger := wf.GetLogger(ctx)
	sel := wf.NewSelector(ctx)

	stageCh := wf.GetSignalChannel(ctx, SignalStageTransition+":"+in.instanceID)
	sel.AddReceive(stageCh, func(c wf.ReceiveChannel, more bool) {
		var sig stageTransitionSignal
		c.Receive(ctx, &sig)
		if err := validateSignal(in.status, SignalStageTransition); err != nil {
			logger.Warn("dropping stage-transition signal", "error", err)
			return
		}
		key := domain.NodeKey(sig.DeptID + "/" + sig.NodeID)
		if sig.NodeID == "" {
			key = domain.NodeKey(sig.DeptID + "/" + sig.ToStage)
		}
		in.lastResultJSON = sig.ResultJSON
		if ch, ok := in.pending[key]; ok {
			ch.Send(ctx, sig)
		} else {
			// No runTaskStage call has registered for this node yet —
			// buffer it so a later registration picks it up immediately
			// instead of silently missing a resolution that arrived first.
			in.pendingSignals[key] = sig
		}
	})

	deferCh := wf.GetSignalChannel(ctx, SignalStageDefer+":"+in.instanceID)
	sel.AddReceive(deferCh, func(c wf.ReceiveChannel, more bool) {
		var sig stageDeferSignal
		c.Receive(ctx, &sig)
		if err := validateSignal(in.status, SignalStageDefer); err != nil {
			logger.Warn("dropping stage-defer signal", "error", err)
			return
		}
		fromNode := domain.NodeKey(sig.DeptID + "/" + sig.FromStage)
		popped := in.history.PopTo(fromNode)
		in.msgBuf.ResetSpan(popped)
		// Simplification: DeferTaskInput's TaskID/AssignmentID are not
		// carried on this signal's payload (LLD §3.1 doesn't specify them
		// here either) — this interpreter uses the from-node key as a
		// stand-in identifier, same simplification runTaskStage makes for
		// CompleteAssignmentActivity. Reconcile with the real convention
		// once the persistence-layer sibling task defines it.
		_, _ = deferTask(ctx, port.DeferTaskInput{
			TaskID: string(fromNode), TenantID: in.tenantID, UserID: sig.UserID,
			AssignmentID: string(fromNode), Reason: sig.Reason, RecordVersion: sig.RecordVersion,
		})
	})

	for _, name := range []string{SignalInstancePause, SignalInstanceResume, SignalInstanceCancel, SignalInstanceForceFwd, SignalInstanceForceBack} {
		ch := wf.GetSignalChannel(ctx, name+":"+in.instanceID)
		sel.AddReceive(ch, func(c wf.ReceiveChannel, more bool) {
			var sig adminSignal
			c.Receive(ctx, &sig)
			if err := validateSignal(in.status, name); err != nil {
				logger.Warn("dropping admin signal", "signal", name, "error", err)
				return
			}
			switch name {
			case SignalInstancePause:
				in.status = domain.InstanceStatusPaused
			case SignalInstanceResume:
				in.status = domain.InstanceStatusRunning
			default:
				// instance-cancel, instance-force-forward, instance-force-back.
				envelope := adminSignalEnvelope{Kind: name, Signal: sig}
				if in.parallelDepth > 0 {
					admin.Send(ctx, envelope)
				} else {
					baseAdmin.Send(ctx, envelope)
				}
			}
		})
	}

	for {
		sel.Select(ctx)
	}
}

// adminSignalEnvelope tags an adminSignal with which signal name delivered
// it, since admin is a single shared channel multiplexing all three.
type adminSignalEnvelope struct {
	Kind   string
	Signal adminSignal
}
