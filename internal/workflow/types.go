package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type ExecuteInput struct {
	TenantID    string
	InstanceID  string
	VersionID   string
	ContextJSON string
	OverrideMap map[string]string
}

type ExecuteOutput struct {
	Status domain.InstanceStatus
}

type stepOutcome struct {
	LastNode   domain.NodeKey
	LastResult string
	Terminated bool
}

type branchOutcome struct {
	Key      string
	DeptID   string
	LastNode domain.NodeKey
	Steps    []dsl.ExecutionStep
	Err      error

	Terminated bool
}

type nodeVisitKey struct {
	Node  domain.NodeKey
	Visit int64
}

type interpreter struct {
	tenantID   string
	instanceID string
	collab     *dsl.CompiledCollaboration

	history *nodeHistory
	msgBuf  *messageBuffer

	admin       wf.Channel
	baseAdmin   wf.Channel
	forkEntries []domain.NodeKey
	routing     bool

	external        map[string]bool
	externalWaiting map[string]wf.Channel

	contextJSON string

	overrideMap map[string]string

	pending map[nodeVisitKey]wf.Channel

	pendingSignals map[nodeVisitKey]stageTransitionSignal
	heldResults    []heldResult

	taskVisits map[domain.NodeKey]int64

	taskAssignees map[string]assigneeNote

	deferredTasks map[string]bool
	deferCh       map[string]wf.Channel
	deferEarly    map[string]bool
	stageExited   map[string]bool

	pauseGates map[string]*pauseGate

	statusRestorePending bool
	finishing            bool
	settleLock           wf.Channel

	status         domain.InstanceStatus
	pauseInitiator string

	pauseHeld          bool
	pauseSignal        adminSignal
	degradedKeepsPause bool
	parks              []*park
	gateways           []*gateway

	parallelDepth int
}

func newInterpreter(tenantID, instanceID, initialContextJSON string, collab *dsl.CompiledCollaboration, overrideMap map[string]string) *interpreter {
	return &interpreter{
		tenantID:        tenantID,
		instanceID:      instanceID,
		collab:          collab,
		history:         newNodeHistory(),
		msgBuf:          newMessageBuffer(),
		external:        externalWaitKeys(collab),
		externalWaiting: make(map[string]wf.Channel),
		contextJSON:     initialContextJSON,
		overrideMap:     overrideMap,
		pending:         make(map[nodeVisitKey]wf.Channel),
		pendingSignals:  make(map[nodeVisitKey]stageTransitionSignal),
		taskVisits:      make(map[domain.NodeKey]int64),
		taskAssignees:   make(map[string]assigneeNote),
		deferredTasks:   make(map[string]bool),
		deferCh:         make(map[string]wf.Channel),
		deferEarly:      make(map[string]bool),
		stageExited:     make(map[string]bool),
		pauseGates:      make(map[string]*pauseGate),
		status:          domain.InstanceStatusRunning,
	}
}

// pauseGate holds a department's branches at their next step. Overlapping
// forks (an inner and an outer rerun) each hold it once; it opens, for every
// branch waiting at it, when the last holder resumes.
type pauseGate struct {
	open  wf.Channel
	holds int
}

// checkPaused waits at the department's pause gate, if one is held. A run that
// is cancelled while it waits ends as abandoned, so the stage that follows never
// runs under a cancelled context.
func (in *interpreter) checkPaused(ctx wf.Context, deptID string) error {
	gate, ok := in.pauseGates[deptID]
	if !ok {
		return nil
	}
	sel := wf.NewSelector(ctx)
	sel.AddReceive(gate.open, func(c wf.ReceiveChannel, more bool) { c.Receive(ctx, nil) })
	sel.AddReceive(ctx.Done(), func(c wf.ReceiveChannel, more bool) {})
	sel.Select(ctx)
	if ctx.Err() != nil {
		return errStageAbandoned
	}
	return nil
}

func (in *interpreter) pauseDept(ctx wf.Context, deptID string) {
	gate, ok := in.pauseGates[deptID]
	if !ok {
		gate = &pauseGate{open: wf.NewChannel(ctx)}
		in.pauseGates[deptID] = gate
	}
	gate.holds++
}

func (in *interpreter) resumeDept(deptID string) {
	gate, ok := in.pauseGates[deptID]
	if !ok {
		return
	}
	if gate.holds--; gate.holds > 0 {
		return
	}
	delete(in.pauseGates, deptID)
	gate.open.Close()
}
