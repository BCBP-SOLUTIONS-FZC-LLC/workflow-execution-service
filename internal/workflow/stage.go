package workflow

import (
	"encoding/json"
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/enums"
)

// stageNodeKey builds this interpreter's NodeKey (execution LLD §4.2):
// dept+NodeID when NodeID is populated (LLD §2.6), else dept+Type — matching
// the once-stage-of-a-type-per-department shape real compiled plans exhibit
// today.
func stageNodeKey(deptID string, stage *dsl.StageDef) domain.NodeKey {
	if stage.NodeID != "" {
		return domain.NodeKey(deptID + "/" + stage.NodeID)
	}
	return domain.NodeKey(deptID + "/" + stage.Type)
}

// runStage dispatches a single compiled stage (LLD §2.4's stage-type
// dispatch table). stage.Type is a plain string, not enums.StageType, by
// design — an unrecognized value is a valid forward-compat passthrough
// (definition_service's compiler emits it as a warning, never an error),
// so this never treats an unknown Type as a failure.
func (in *interpreter) runStage(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, stage *dsl.StageDef) (domain.NodeKey, error) {
	nodeKey := stageNodeKey(deptID, stage)

	switch stage.Type {
	case string(enums.StageTypeSendTask):
		msgName := stage.Extras["message"]
		in.msgBuf.Send(ctx, msgName, nodeKey)
		in.history.Push(nodeKey)
		return nodeKey, nil

	case string(enums.StageTypeReceiveTask):
		msgName := stage.Extras["message"]
		in.msgBuf.Receive(ctx, msgName)
		in.history.Push(nodeKey)
		return nodeKey, nil

	default:
		if stage.EngineNote != "" {
			wf.GetLogger(ctx).Warn("dispatching stage with engine note", "node_key", string(nodeKey), "engine_note", stage.EngineNote)
		}
		return in.runTaskStage(ctx, plan, stage, nodeKey)
	}
}

// runTaskStage handles prep/review/approve/unrecognized-Type dispatch (LLD
// §2.2/§3.4).
func (in *interpreter) runTaskStage(ctx wf.Context, plan *dsl.CompiledPlan, stage *dsl.StageDef, nodeKey domain.NodeKey) (domain.NodeKey, error) {
	compiledNode, err := json.Marshal(stage)
	if err != nil {
		return nodeKey, fmt.Errorf("workflow: marshaling stage %q: %w", nodeKey, err)
	}
	out, err := createTask(ctx, port.CreateTaskInput{
		InstanceID:   in.instanceID,
		TenantID:     in.tenantID,
		NodeKey:      nodeKey,
		CompiledNode: compiledNode,
		ContextJSON:  in.contextJSON,
	})
	if err != nil {
		return nodeKey, err
	}
	if err := updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{nodeKey},
	}); err != nil {
		return nodeKey, err
	}

	resolveCh := wf.NewBufferedChannel(ctx, 1)
	if buffered, ok := in.pendingSignals[nodeKey]; ok {
		delete(in.pendingSignals, nodeKey)
		resolveCh.Send(ctx, buffered)
	}
	in.pending[nodeKey] = resolveCh
	defer delete(in.pending, nodeKey)

	sel := wf.NewSelector(ctx)
	var resolved bool
	var sig stageTransitionSignal
	sel.AddReceive(resolveCh, func(c wf.ReceiveChannel, more bool) {
		c.Receive(ctx, &sig)
		resolved = true
	})

	var fired *boundaryFire
	cancelBoundaries := registerTaskBoundaries(ctx, sel, in.msgBuf, stage.BoundaryTimer, stage.BoundaryMessage, func(f boundaryFire) {
		fired = &f
	})
	cancelSLA := addSLATimers(ctx, sel, slaTimerParams{
		TenantID: in.tenantID, InstanceID: in.instanceID, TaskID: out.TaskID, NodeKey: nodeKey,
		DueDate: stage.DueDate, FollowUpDate: stage.FollowUpDate,
	})

	for !resolved && fired == nil {
		sel.Select(ctx)
	}
	cancelSLA()

	if fired != nil {
		cancelBoundaries()
		if fired.Interrupting {
			delete(in.pending, nodeKey)
			// The task row createTask created above is now abandoned — mark
			// it SUPERSEDED so it doesn't sit open/claimable forever with no
			// terminal status. Best-effort: an error here would abort the
			// whole instance over what's ultimately an audit/bookkeeping
			// write, a worse outcome than proceeding with the boundary
			// transfer the compiled plan actually calls for.
			_ = updateTaskStatus(ctx, port.UpdateTaskStatusInput{
				TaskID: out.TaskID, TenantID: in.tenantID, Status: domain.TaskStatusSuperseded,
			})
			return in.runDepartment(ctx, plan, fired.TargetDept)
		}
		// Non-interrupting: the host keeps running (we keep waiting below).
		in.spawnNonInterruptingTarget(ctx, plan, fired.TargetDept)
		resolveCh.Receive(ctx, &sig)
	} else {
		cancelBoundaries()
	}

	// Simplification: CreateTaskOutput carries only TaskID, not a
	// per-assignee AssignmentID — this interpreter uses TaskID as the
	// single-assignee AssignmentID reference. Multi-assignee claim/lead
	// bookkeeping (ClaimAssignmentActivity) is a separate, additive path
	// this task does not need to invoke for the common single-assignee
	// case. Reconcile with whichever sibling task owns the real assignment
	// ID convention if this proves wrong.
	if _, err := completeAssignment(ctx, port.CompleteAssignmentInput{
		AssignmentID: out.TaskID, TenantID: in.tenantID, ResultJSON: sig.ResultJSON, RecordVersion: sig.RecordVersion,
	}); err != nil {
		return nodeKey, err
	}

	in.lastResultJSON = sig.ResultJSON
	in.history.Push(nodeKey)
	return nodeKey, nil
}
