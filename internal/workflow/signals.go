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

// signalPreconditions is the instance-status precondition each signal
// requires, and the ONLY gate a signal passes through before reaching any
// Selector (execution LLD §3.1, §7.2 test #5).
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
// TargetDeptID isn't in the LLD's literal §3.1 payload table — added to
// disambiguate which failed branch a signal applies to when DEGRADED has
// more than one; a documented, smallest-reasonable assumption pending
// confirmation from whichever task owns the real signal payload contract.
type adminSignal struct {
	AdminUserID   string
	Reason        string
	TargetDeptID  string
	TargetNodeKey domain.NodeKey // set only for instance-force-forward
	RecordVersion int64
}

// runSignalRouter is the always-on background goroutine (started once, in
// Execute) draining every raw signal channel for this instance, validating
// each before anything else can react to it (execution LLD §7.2 test #5).
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
		// fromNode names the currently-pending stage being deferred — never
		// yet in history.Push'd (that only happens once a stage completes,
		// stage.go), so there's nothing of its own to PopTo here. Any
		// regression-task bookkeeping belongs to DeferTaskActivity itself
		// (a persistence-layer concern, not yet built).
		fromNode := domain.NodeKey(sig.DeptID + "/" + sig.FromStage)
		// Simplification: uses the from-node key as a stand-in Task/AssignmentID
		// (same as runTaskStage's CompleteAssignmentActivity call) until the
		// persistence-layer sibling task defines the real convention.
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
