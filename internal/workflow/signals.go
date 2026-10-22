package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

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

	SignalInstanceExternalAnswer: {domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
}

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

func canResume(pausedBy, resumeInitiator string) bool {
	if resumeInitiator == domain.InitiatorAdmin || resumeInitiator == "" {
		return true
	}
	return resumeInitiator == pausedBy
}

type stageTransitionSignal struct {
	DeptID        string
	ToStage       string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
	Failed        bool
	Reason        string
	VisitCount    int64
}

type stageFailSignal struct {
	DeptID        string `json:"dept_id"`
	NodeID        string `json:"node_id"`
	ConnectorType string `json:"connector_type"`
	ErrorClass    string `json:"error_class"`
	RecordVersion int64  `json:"record_version"`
	VisitCount    int64  `json:"visit_count"`
}

type stageDeferSignal struct {
	DeptID        string
	FromStage     string
	Reason        string
	UserID        string
	RecordVersion int64
	TaskID        string
	AssignmentID  string
}

type reassignSignal struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
}

type adminSignal struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetDeptID  string
	TargetNodeKey domain.NodeKey
	RecordVersion int64
	FromDefer     bool
}

func (in *interpreter) runSignalRouter(ctx wf.Context, admin, baseAdmin wf.Channel) {
	in.admin, in.baseAdmin = admin, baseAdmin
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

	externalCh := wf.GetSignalChannel(ctx, SignalInstanceExternalAnswer+":"+in.instanceID)
	sel.AddReceive(externalCh, func(c wf.ReceiveChannel, more bool) {
		var sig externalAnswerSignal
		c.Receive(ctx, &sig)
		in.handleExternalAnswer(ctx, sig)
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
			if in.holdsPauseDuringPark(name) {
				in.recordHeldPause(ctx, name, sig)
				return
			}
			if err := validateSignal(in.status, name); err != nil {
				logger.Warn("dropping admin signal", "signal", name, "error", err)
				return
			}
			switch name {
			case SignalInstancePause:
				in.handleInstancePause(ctx, sig)
			case SignalInstanceResume:
				if !canResume(in.pauseInitiator, sig.Initiator) {
					logger.Warn("dropping instance-resume signal: initiator does not own this pause",
						"resume_initiator", sig.Initiator, "paused_by", in.pauseInitiator)
					return
				}
				in.handleInstanceResume(ctx, sig)
			default:
				if sig.TargetDeptID == "" && sig.TargetNodeKey != "" && getVersion(ctx, forceRouteByNodeKeyChangeID) != wf.DefaultVersion {
					sig.TargetDeptID = deptFromNodeKey(sig.TargetNodeKey)
				}
				in.routeAdminSignal(ctx, name, sig)
			}
		})
	}

	for {
		sel.Select(ctx)
	}
}

func (in *interpreter) handleStageTransition(ctx wf.Context, sig stageTransitionSignal) {
	if err := validateSignal(in.status, SignalStageTransition); err != nil {
		wf.GetLogger(ctx).Warn("dropping stage-transition signal", "error", err)
		return
	}
	key := domain.NodeKey(sig.DeptID + "/" + sig.NodeID)
	if sig.NodeID == "" {
		key = domain.NodeKey(sig.DeptID + "/" + sig.ToStage)
	}
	visit := int64(0)
	if getVersion(ctx, nodeVisitKeyChangeID) != wf.DefaultVersion {
		visit = sig.VisitCount
	}
	visitKey := nodeVisitKey{Node: key, Visit: visit}
	if ch, ok := in.pending[visitKey]; ok {
		ch.Send(ctx, sig)
	} else {
		in.pendingSignals[visitKey] = sig
	}
}

func (in *interpreter) handleStageFail(ctx wf.Context, sig stageFailSignal) {
	if err := validateSignal(in.status, SignalStageFail); err != nil {
		wf.GetLogger(ctx).Warn("dropping stage-fail signal", "error", err)
		return
	}
	transition := stageTransitionSignal{
		DeptID: sig.DeptID, NodeID: sig.NodeID, Failed: true, Reason: sig.ErrorClass, RecordVersion: sig.RecordVersion,
		VisitCount: sig.VisitCount,
	}
	key := domain.NodeKey(sig.DeptID + "/" + sig.NodeID)
	visit := int64(0)
	if getVersion(ctx, nodeVisitKeyChangeID) != wf.DefaultVersion {
		visit = sig.VisitCount
	}
	visitKey := nodeVisitKey{Node: key, Visit: visit}
	if ch, ok := in.pending[visitKey]; ok {
		ch.Send(ctx, transition)
	} else {
		in.pendingSignals[visitKey] = transition
	}
}

func (in *interpreter) routeAdminSignal(ctx wf.Context, kind string, sig adminSignal) {
	envelope := adminSignalEnvelope{Kind: kind, Signal: sig}
	if in.parallelDepth > 0 {
		in.admin.Send(ctx, envelope)
	} else {
		in.baseAdmin.Send(ctx, envelope)
	}
}

func (in *interpreter) handleStageDefer(ctx wf.Context, sig stageDeferSignal) {
	if err := validateSignal(in.status, SignalStageDefer); err != nil {
		wf.GetLogger(ctx).Warn("dropping stage-defer signal", "error", err)
		return
	}
	input := port.DeferTaskInput{
		TaskID: sig.TaskID, TenantID: in.tenantID, UserID: sig.UserID,
		AssignmentID: sig.AssignmentID, Reason: sig.Reason, RecordVersion: sig.RecordVersion,
	}
	if getVersion(ctx, deferRoutesBackChangeID) == wf.DefaultVersion {
		_ = deferTask(ctx, input)
		return
	}
	if in.status != domain.InstanceStatusRunning {
		wf.GetLogger(ctx).Warn("dropping stage-defer signal: only a running instance goes back a step", "status", in.status)
		return
	}
	target := in.history.Peek()
	if in.parallelDepth > 0 {
		target = in.forkEntries[len(in.forkEntries)-1]
	}
	if target == "" {
		wf.GetLogger(ctx).Warn("dropping stage-defer signal: no completed step to go back to")
		return
	}
	input.DeferredToNodeKey = string(target)
	if err := deferTask(ctx, input); err != nil {
		wf.GetLogger(ctx).Warn("deferTask failed", "error", err)
		return
	}
	in.routeAdminSignal(ctx, SignalInstanceForceBack, adminSignal{
		AdminUserID: sig.UserID, Reason: sig.Reason, RecordVersion: sig.RecordVersion, FromDefer: true,
	})
}

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

func (in *interpreter) holdsPauseDuringPark(signal string) bool {
	return in.status == domain.InstanceStatusDegraded && in.degradedKeepsPause &&
		(signal == SignalInstancePause || signal == SignalInstanceResume)
}

func (in *interpreter) recordHeldPause(ctx wf.Context, signal string, sig adminSignal) {
	logger := wf.GetLogger(ctx)
	if signal == SignalInstancePause {
		if in.pauseHeld {
			logger.Warn("dropping instance-pause signal: a pause is already held", "paused_by", in.pauseInitiator)
			return
		}
		in.pauseHeld = true
		in.pauseInitiator = sig.Initiator
		in.pauseSignal = sig
		return
	}
	if !in.pauseHeld {
		logger.Warn("dropping instance-resume signal: no pause is held")
		return
	}
	if !canResume(in.pauseInitiator, sig.Initiator) {
		logger.Warn("dropping instance-resume signal: initiator does not own the held pause",
			"resume_initiator", sig.Initiator, "paused_by", in.pauseInitiator)
		return
	}
	in.pauseHeld = false
	in.pauseInitiator = ""
	in.pauseSignal = adminSignal{}
}

func (in *interpreter) handleInstancePause(ctx wf.Context, sig adminSignal) {
	in.status = domain.InstanceStatusPaused
	in.pauseInitiator = sig.Initiator
	in.pauseHeld = true
	in.pauseSignal = sig
	if err := pauseInstance(ctx, port.PauseInstanceInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		AdminUserID: sig.AdminUserID, Initiator: sig.Initiator, RecordVersion: sig.RecordVersion,
	}); err != nil {
		wf.GetLogger(ctx).Warn("pauseInstance failed", "error", err)
	}
}

func (in *interpreter) handleInstanceResume(ctx wf.Context, sig adminSignal) {
	in.status = domain.InstanceStatusRunning
	in.pauseInitiator = ""
	in.pauseHeld = false
	in.pauseSignal = adminSignal{}
	if err := resumeInstance(ctx, port.ResumeInstanceInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		AdminUserID: sig.AdminUserID, Initiator: sig.Initiator, RecordVersion: sig.RecordVersion,
	}); err != nil {
		wf.GetLogger(ctx).Warn("resumeInstance failed", "error", err)
	}
}

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

type adminSignalEnvelope struct {
	Kind   string
	Signal adminSignal
}
