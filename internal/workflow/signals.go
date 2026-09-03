package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// Signal name prefixes, per LLD §3.1's signal catalogue. Every channel this
// package listens on is named prefix + ":" + instanceID — even
// instance-reassign, whose payload identifies one specific task (TaskID),
// not the whole instance. Temporal signals a workflow execution by
// workflowID (== instanceID here), so a caller must resolve taskID ->
// instanceID before signaling regardless of how the channel itself is
// named; keying the channel by taskID would buy nothing. stage-transition/
// stage-defer share this same instanceID-keyed channel and instead route to
// one specific in-flight runTaskStage call via the pending/pendingSignals
// maps below; instance-reassign has no in-flight call to resolve — it's a
// pure side-effecting DB write — so it skips that indirection entirely (see
// handleInstanceReassign).
const (
	SignalStageTransition   = "stage-transition"
	SignalStageDefer        = "stage-defer"
	SignalStageFail         = "stage-fail"
	SignalInstancePause     = "instance-pause"
	SignalInstanceResume    = "instance-resume"
	SignalInstanceCancel    = "instance-cancel"
	SignalInstanceForceFwd  = "instance-force-forward"
	SignalInstanceForceBack = "instance-force-back"
	SignalInstanceReassign  = "instance-reassign"
)

// signalPreconditions is the instance-status precondition each signal
// requires, and the ONLY gate a signal passes through before reaching any
// Selector (execution LLD §3.1, §7.2 test #5).
var signalPreconditions = map[string][]domain.InstanceStatus{
	SignalStageTransition:   {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalStageDefer:        {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalStageFail:         {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalInstancePause:     {domain.InstanceStatusRunning},
	SignalInstanceResume:    {domain.InstanceStatusPaused},
	SignalInstanceCancel:    {domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded},
	SignalInstanceForceFwd:  {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalInstanceForceBack: {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
	SignalInstanceReassign:  {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
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
	Failed        bool
	Reason        string
}

// stageFailSignal is the wire payload of SignalStageFail — execution_service.md
// §3.1's documented shape, snake_case json tags included. A dedicated struct
// rather than reusing stageTransitionSignal: no sender exists anywhere yet
// (cmd/connector-worker, this signal's only intended caller, isn't built),
// so there is no wire format to preserve here — unlike
// SignalStageTransition, which has real replay history and must not have
// its own wire encoding touched. handleStageFail converts this into a
// stageTransitionSignal internally so runTaskStage's resolution/pending-map
// machinery (typed against stageTransitionSignal, with real replay history
// of its own) needs no changes at all.
type stageFailSignal struct {
	DeptID        string `json:"dept_id"`
	NodeID        string `json:"node_id"`
	ConnectorType string `json:"connector_type"`
	ErrorClass    string `json:"error_class"`
	RecordVersion int64  `json:"record_version"`
}

// stageDeferSignal is the payload of SignalStageDefer (LLD §3.1).
type stageDeferSignal struct {
	DeptID        string
	FromStage     string
	Reason        string
	UserID        string
	RecordVersion int64
}

// reassignSignal is the payload of SignalInstanceReassign (LLD §3.1): unlike
// adminSignal's instance-wide scope, it identifies one specific task
// (TaskID) whose assignment is being vacated and re-inserted for a new
// user — the node-override HTTP path's eventual runtime counterpart.
type reassignSignal struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
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
	Initiator     string // set only for instance-pause/instance-resume
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
		in.handleStageTransition(ctx, sig)
	})

	deferCh := wf.GetSignalChannel(ctx, SignalStageDefer+":"+in.instanceID)
	sel.AddReceive(deferCh, func(c wf.ReceiveChannel, more bool) {
		var sig stageDeferSignal
		c.Receive(ctx, &sig)
		in.handleStageDefer(ctx, sig)
	})

	failCh := wf.GetSignalChannel(ctx, SignalStageFail+":"+in.instanceID)
	sel.AddReceive(failCh, func(c wf.ReceiveChannel, more bool) {
		var sig stageFailSignal
		c.Receive(ctx, &sig)
		in.handleStageFail(ctx, sig)
	})

	reassignCh := wf.GetSignalChannel(ctx, SignalInstanceReassign+":"+in.instanceID)
	sel.AddReceive(reassignCh, func(c wf.ReceiveChannel, more bool) {
		var sig reassignSignal
		c.Receive(ctx, &sig)
		in.handleInstanceReassign(ctx, sig)
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
				in.handleInstancePause(ctx, sig)
			case SignalInstanceResume:
				in.handleInstanceResume(ctx, sig)
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

// handleStageTransition resolves a stage-transition signal against whichever
// runTaskStage call is (or isn't yet) waiting on this node.
func (in *interpreter) handleStageTransition(ctx wf.Context, sig stageTransitionSignal) {
	if err := validateSignal(in.status, SignalStageTransition); err != nil {
		wf.GetLogger(ctx).Warn("dropping stage-transition signal", "error", err)
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
}

// handleStageFail resolves a stage-fail signal against whichever
// runTaskStage call is (or isn't yet) waiting on this node — the same
// task-level correlation pattern handleStageTransition uses above, not the
// instance-wide adminSignal pattern pause/resume/cancel use, since a
// stage-fail signal always targets one specific in-flight task. Converts the
// wire-shaped stageFailSignal into a stageTransitionSignal (Failed=true,
// Reason=sig.ErrorClass) so runTaskStage's resolution/pending-map machinery
// — typed against stageTransitionSignal, with real replay history of its
// own — needs no changes for this. ConnectorType isn't consumed here:
// runTaskStage's resolution doesn't need it, only the sender did.
func (in *interpreter) handleStageFail(ctx wf.Context, sig stageFailSignal) {
	if err := validateSignal(in.status, SignalStageFail); err != nil {
		wf.GetLogger(ctx).Warn("dropping stage-fail signal", "error", err)
		return
	}
	transition := stageTransitionSignal{
		DeptID: sig.DeptID, NodeID: sig.NodeID, Failed: true, Reason: sig.ErrorClass, RecordVersion: sig.RecordVersion,
	}
	key := domain.NodeKey(sig.DeptID + "/" + sig.NodeID)
	if ch, ok := in.pending[key]; ok {
		ch.Send(ctx, transition)
	} else {
		in.pendingSignals[key] = transition
	}
}

// handleStageDefer closes the deferring assignment(s) for a stage-defer
// signal.
func (in *interpreter) handleStageDefer(ctx wf.Context, sig stageDeferSignal) {
	if err := validateSignal(in.status, SignalStageDefer); err != nil {
		wf.GetLogger(ctx).Warn("dropping stage-defer signal", "error", err)
		return
	}
	// fromNode names the currently-pending stage being deferred — never yet
	// in history.Push'd (that only happens once a stage completes, stage.go),
	// so there's nothing of its own to PopTo here: an earlier version of
	// this handler called history.PopTo(fromNode) anyway, and PopTo's own
	// not-found fallback then wiped the ENTIRE history stack, including
	// unrelated already-completed nodes. Any regression-task bookkeeping
	// belongs to DeferTaskActivity itself (a persistence-layer concern, not
	// yet built).
	fromNode := domain.NodeKey(sig.DeptID + "/" + sig.FromStage)
	// Simplification: uses the from-node key as a stand-in Task/AssignmentID
	// (same as runTaskStage's CompleteAssignmentActivity call) until the
	// persistence-layer sibling task defines the real convention.
	_, _ = deferTask(ctx, port.DeferTaskInput{
		TaskID: string(fromNode), TenantID: in.tenantID, UserID: sig.UserID,
		AssignmentID: string(fromNode), Reason: sig.Reason, RecordVersion: sig.RecordVersion,
	})
}

// handleInstanceReassign calls ReassignAssignmentActivity (vacates the old
// assignment, inserts the new one) for an instance-reassign signal.
// Best-effort, mirroring handleStageDefer's own convention above: unlike
// cancelInstanceOnSignal's terminal-state write, a failure here isn't fatal
// to the instance, so it's logged and the workflow keeps running.
func (in *interpreter) handleInstanceReassign(ctx wf.Context, sig reassignSignal) {
	if err := validateSignal(in.status, SignalInstanceReassign); err != nil {
		wf.GetLogger(ctx).Warn("dropping instance-reassign signal", "error", err)
		return
	}
	if err := reassignAssignment(ctx, port.ReassignAssignmentInput{
		TaskID: sig.TaskID, TenantID: in.tenantID, OldUserID: sig.OldUserID,
		NewUserID: sig.NewUserID, AdminUserID: sig.AdminUserID, RecordVersion: sig.RecordVersion,
	}); err != nil {
		wf.GetLogger(ctx).Warn("reassignAssignment failed", "error", err)
	}
}

// handleInstancePause flips the in-memory status gate (checked by
// validateSignal) and persists the pause + writes the matching event via
// PauseInstanceActivity. A persistence failure is logged, not propagated —
// mirroring stage-defer's own best-effort convention above; the signal was
// already synchronously version-checked at the HTTP layer before being sent.
func (in *interpreter) handleInstancePause(ctx wf.Context, sig adminSignal) {
	in.status = domain.InstanceStatusPaused
	if err := pauseInstance(ctx, port.PauseInstanceInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		AdminUserID: sig.AdminUserID, Initiator: sig.Initiator, RecordVersion: sig.RecordVersion,
	}); err != nil {
		wf.GetLogger(ctx).Warn("pauseInstance failed", "error", err)
	}
}

// handleInstanceResume is handleInstancePause's mirror image.
func (in *interpreter) handleInstanceResume(ctx wf.Context, sig adminSignal) {
	in.status = domain.InstanceStatusRunning
	if err := resumeInstance(ctx, port.ResumeInstanceInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		AdminUserID: sig.AdminUserID, Initiator: sig.Initiator, RecordVersion: sig.RecordVersion,
	}); err != nil {
		wf.GetLogger(ctx).Warn("resumeInstance failed", "error", err)
	}
}

// cancelInstanceOnSignal calls CancelInstanceActivity (task-FAILED cascade +
// assignment vacate + TERMINATED status + both event classes, in one write)
// for an instance-cancel signal received at any of runTopLevel/runParallel/
// enterDegraded. Its error, unlike pause/resume's, is not swallowed — it
// propagates as stepOutcome's own error so Execute's runErr handling can
// fall back to FAILED rather than reporting a false Terminated success.
func (in *interpreter) cancelInstanceOnSignal(ctx wf.Context, sig adminSignal) error {
	var reason *string
	if sig.Reason != "" {
		reason = &sig.Reason
	}
	return cancelInstance(ctx, port.CancelInstanceInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		AdminUserID: sig.AdminUserID, Reason: reason, RecordVersion: sig.RecordVersion,
	})
}

// adminSignalEnvelope tags an adminSignal with which signal name delivered
// it, since admin is a single shared channel multiplexing all three.
type adminSignalEnvelope struct {
	Kind   string
	Signal adminSignal
}
