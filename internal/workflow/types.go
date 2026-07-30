package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// ExecuteInput is the input to the Execute workflow function.
type ExecuteInput struct {
	TenantID    string
	InstanceID  string
	VersionID   string
	ContextJSON string
}

// ExecuteOutput is the terminal output of the Execute workflow function.
type ExecuteOutput struct {
	Status domain.InstanceStatus
}

// stepOutcome is what runSteps returns: the last node reached, and whether an
// ExclusiveBranch with Terminates:true ended the plan early.
type stepOutcome struct {
	LastNode   domain.NodeKey
	Terminated bool
}

// branchOutcome is what a Parallel branch's goroutine reports back to the
// aggregator (dispatch.go), consumed by the DEGRADED aggregator (degraded.go).
type branchOutcome struct {
	DeptID   string
	LastNode domain.NodeKey
	Err      error
}

// interpreter is the workflow-local state for one instance: not safe to
// share outside its own workflow execution (fine, since Temporal coroutines
// are cooperatively scheduled, never truly concurrent).
type interpreter struct {
	tenantID   string
	instanceID string
	collab     *dsl.CompiledCollaboration

	history *nodeHistory
	msgBuf  *messageBuffer

	// Instance-wide shared state rather than per-branch scoped — the DSL
	// doesn't specify per-branch variable scoping (LLD §2.4, same philosophy
	// as messageBuffer). contextJSON feeds CreateTaskActivity; lastResultJSON
	// is the just-completed stage's result, read by the exclusive-gateway
	// evaluator.
	contextJSON    string
	lastResultJSON string

	// pending: node key -> the channel a runTaskStage call is blocked on,
	// so signals.go's router can forward a stage-transition signal to it.
	pending map[domain.NodeKey]wf.Channel

	// pendingSignals buffers a stage-transition signal that arrives before
	// its runTaskStage call has registered in pending, so it isn't silently
	// dropped — same "buffer if no receiver yet" shape as messageBuffer.
	pendingSignals map[domain.NodeKey]stageTransitionSignal

	// pauseGates: department -> one-shot resume channel while that
	// department's branch is paused (not cancelled) during a force-back
	// that arrived while a parallel gateway was active (LLD §2.7).
	pauseGates map[string]wf.Channel

	status domain.InstanceStatus

	// parallelDepth >0 while a Parallel gateway is aggregating. signals.go
	// uses it to route each cancel/force-forward/force-back signal to
	// exactly one of admin (runParallel/enterDegraded) or baseAdmin
	// (runTopLevel) — never both, which would race for delivery.
	//
	// Known gap: this is a flat counter, not a per-nesting-level lock. If a
	// ParallelBranch's own steps (or a SubWorkflow/CallPool inlined beneath
	// one) contain ANOTHER Parallel gateway, both levels' runParallel/
	// enterDegraded loops register concurrent Selector cases on the same
	// admin channel while both are active, and a single signal is delivered
	// to only one of them — not necessarily the level it was addressed to.
	// Nested Parallel gateways are not exercised by any fixture today; this
	// would need per-nesting-level channel scoping to close properly.
	parallelDepth int
}

func newInterpreter(tenantID, instanceID, initialContextJSON string, collab *dsl.CompiledCollaboration) *interpreter {
	return &interpreter{
		tenantID:       tenantID,
		instanceID:     instanceID,
		collab:         collab,
		history:        newNodeHistory(),
		msgBuf:         newMessageBuffer(),
		contextJSON:    initialContextJSON,
		pending:        make(map[domain.NodeKey]wf.Channel),
		pendingSignals: make(map[domain.NodeKey]stageTransitionSignal),
		pauseGates:     make(map[string]wf.Channel),
		status:         domain.InstanceStatusRunning,
	}
}

// checkPaused blocks if deptID is currently paused by an in-flight
// force-back (LLD §2.7 point 2's "checked between steps" mechanism) —
// checked only at stage boundaries, between one stage finishing and the
// next starting, rather than via cancellation, so a paused branch's
// goroutine state stays valid for a later resume. This means pausing has no
// effect on a branch already inside a stage's own wait (the common case —
// most of a stage's lifetime) until that stage resolves naturally; it only
// stops the branch from *starting its next* stage.
func (in *interpreter) checkPaused(ctx wf.Context, deptID string) {
	if ch, ok := in.pauseGates[deptID]; ok {
		ch.Receive(ctx, nil)
		delete(in.pauseGates, deptID)
	}
}

func (in *interpreter) pauseDept(ctx wf.Context, deptID string) {
	in.pauseGates[deptID] = wf.NewBufferedChannel(ctx, 1)
}

func (in *interpreter) resumeDept(ctx wf.Context, deptID string) {
	if ch, ok := in.pauseGates[deptID]; ok {
		ch.Send(ctx, nil)
	}
}
