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
	// OverrideMap is POST /instances' node-key -> user-ID override map
	// (already persisted to workflow_instance.override_map by the HTTP/DB
	// layer). Threaded through, unmodified, to every CreateTaskActivity call
	// so the Activity itself can apply whichever entry (if any) matches the
	// node being dispatched.
	OverrideMap map[string]string
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

	// Instance-wide, not per-branch scoped (LLD §2.4).
	contextJSON    string
	lastResultJSON string

	// overrideMap is ExecuteInput.OverrideMap, carried as-is for the
	// instance's entire lifetime and passed whole to every CreateTaskInput
	// (LLD §5.4's node-key -> user-ID override).
	overrideMap map[string]string

	// pending: node key -> the channel a runTaskStage call is blocked on.
	pending map[domain.NodeKey]wf.Channel

	// pendingSignals buffers a stage-transition signal that arrives before
	// its runTaskStage call has registered in pending.
	pendingSignals map[domain.NodeKey]stageTransitionSignal

	// taskVisits counts runTaskStage calls per NodeKey within this
	// instance's lifetime — CreateTaskInput.VisitCount's source. A node
	// revisited via an exclusive-gateway back-edge (dispatch.go's
	// runExclusiveRevert) or an admin instance-force-back signal gets a
	// fresh count, so CreateTaskActivity's deterministic task ID
	// (instanceID+NodeKey+VisitCount) distinguishes that legitimate second
	// task from a Temporal at-least-once retry of the same runTaskStage
	// call, which always replays the same count.
	taskVisits map[domain.NodeKey]int64

	// pauseGates: department -> one-shot resume channel for a branch paused
	// by an active-parallel-gateway force-back (LLD §2.7).
	pauseGates map[string]wf.Channel

	// callPoolVisits: pool name -> how many times runCallPool (callpool.go)
	// has dispatched an Ignored target for it — qualifies the synthetic
	// admin-stub task's NodeID so two concurrent Parallel branches calling
	// the same pool never collide on one NodeKey.
	callPoolVisits map[string]int64

	status domain.InstanceStatus

	// parallelDepth >0 while a Parallel gateway is aggregating; routes each
	// admin signal to admin (runParallel/enterDegraded) or baseAdmin
	// (runTopLevel), never both.
	//
	// Known gap: a flat counter, not per-nesting-level — a Parallel gateway
	// nested inside another's branch delivers a signal to only one level,
	// not necessarily the addressed one. Not exercised by any fixture today
	// (execution LLD §2.5).
	parallelDepth int
}

func newInterpreter(tenantID, instanceID, initialContextJSON string, collab *dsl.CompiledCollaboration, overrideMap map[string]string) *interpreter {
	return &interpreter{
		tenantID:       tenantID,
		instanceID:     instanceID,
		collab:         collab,
		history:        newNodeHistory(),
		msgBuf:         newMessageBuffer(),
		contextJSON:    initialContextJSON,
		overrideMap:    overrideMap,
		pending:        make(map[domain.NodeKey]wf.Channel),
		pendingSignals: make(map[domain.NodeKey]stageTransitionSignal),
		taskVisits:     make(map[domain.NodeKey]int64),
		pauseGates:     make(map[string]wf.Channel),
		callPoolVisits: make(map[string]int64),
		status:         domain.InstanceStatusRunning,
	}
}

// checkPaused blocks if deptID is paused by an in-flight force-back,
// checked only between stages, never via cancellation (LLD §2.7 point 2).
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
