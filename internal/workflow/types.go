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

	taskVisits map[domain.NodeKey]int64

	pauseGates map[string]wf.Channel

	status         domain.InstanceStatus
	pauseInitiator string

	pauseHeld          bool
	pauseSignal        adminSignal
	degradedKeepsPause bool

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
		pauseGates:      make(map[string]wf.Channel),
		status:          domain.InstanceStatusRunning,
	}
}

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
