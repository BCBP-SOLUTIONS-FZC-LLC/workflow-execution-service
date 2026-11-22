package workflow

import (
	"encoding/json"
	"errors"
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/enums"
)

var errStageAbandoned = errors.New("workflow: stage abandoned")

// errStageDeferred ends a stage whose task was deferred inside a running fork;
// its department runs the stage again once the fork's saved branches resume.
var errStageDeferred = errors.New("workflow: stage deferred")

func (in *interpreter) runStage(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, stage *dsl.StageDef) (domain.NodeKey, string, error) {
	nodeKey := domain.NodeKey(dsl.NodeKey(deptID, stage))

	switch stage.Type {
	case string(enums.StageTypeSendTask):
		msgName := stage.Extras["message"]
		in.msgBuf.Send(ctx, msgName, nodeKey)
		in.history.Push(nodeKey)
		return nodeKey, "", nil

	case string(enums.StageTypeReceiveTask):
		if err := updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
			InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{nodeKey},
		}); err != nil {
			return nodeKey, "", err
		}
		in.receive(ctx, nodeKey, stage.Extras["message"])
		in.history.Push(nodeKey)
		return nodeKey, "", nil

	default:
		if stage.EngineNote != "" {
			wf.GetLogger(ctx).Warn("dispatching stage with engine note", "node_key", string(nodeKey), "engine_note", stage.EngineNote)
		}
		return in.runTaskStage(ctx, plan, deptID, stage, nodeKey)
	}
}

// countOpenTaskStage marks a task stage as open until the returned release
// runs (once). A stage releases before it hands over to a boundary path: that
// path can wait on a receive that a cancel does not wake, and its own task
// stages are counted by themselves.
func (in *interpreter) countOpenTaskStage() (release func()) {
	in.openTaskStages++
	released := false
	return func() {
		if !released {
			released = true
			in.openTaskStages--
		}
	}
}

// awaitAbandonedStages holds a finishing run until every cancelled task stage
// has returned, so its supersede write (including the repeat on a context that
// outlives the cancel) is not cut off by the run completing. The caller has
// cancelled the stages' context, so none of them waits for a person.
func (in *interpreter) awaitAbandonedStages(ctx wf.Context) {
	_ = wf.Await(ctx, func() bool { return in.openTaskStages == 0 })
}

func (in *interpreter) runTaskStage(ctx wf.Context, plan *dsl.CompiledPlan, deptID string, stage *dsl.StageDef, nodeKey domain.NodeKey) (domain.NodeKey, string, error) {
	compiledNode, err := json.Marshal(stage)
	if err != nil {
		return nodeKey, "", fmt.Errorf("workflow: marshaling stage %q: %w", nodeKey, err)
	}
	in.taskVisits[nodeKey]++
	releaseOpenStage := in.countOpenTaskStage()
	defer releaseOpenStage()

	var iamDeptID string
	if dept := findDepartment(plan, deptID); dept != nil {
		iamDeptID = dept.IAMDepartmentID
	}

	out, err := createTask(ctx, port.CreateTaskInput{
		InstanceID:      in.instanceID,
		TenantID:        in.tenantID,
		NodeKey:         nodeKey,
		CompiledNode:    compiledNode,
		ContextJSON:     in.contextJSON,
		OverrideMap:     in.overrideMap,
		VisitCount:      in.taskVisits[nodeKey],
		IAMDepartmentID: iamDeptID,
	})
	if err := in.settleCreatedTask(ctx, out, err); err != nil {
		return nodeKey, "", err
	}
	defer delete(in.taskAssignees, out.TaskID)
	defer func() {
		delete(in.deferEarly, out.TaskID)
		in.stageExited[out.TaskID] = true
	}()
	if err := updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{nodeKey},
	}); err != nil {
		if abandonedMidActivity(ctx) {
			// Not pending yet, so a force route's audit does not list this task.
			in.supersedeTask(ctx, out.TaskID)
			return nodeKey, "", errStageAbandoned
		}
		return nodeKey, "", err
	}

	visitKey := nodeVisitKey{Node: nodeKey, Visit: in.taskVisits[nodeKey]}
	resolveCh := wf.NewBufferedChannel(ctx, 1)
	if buffered, ok := in.pendingSignals[visitKey]; ok {
		delete(in.pendingSignals, visitKey)
		resolveCh.Send(ctx, buffered)
	}
	in.pending[visitKey] = resolveCh
	defer delete(in.pending, visitKey)
	deferCh := wf.NewBufferedChannel(ctx, 1)
	in.deferCh[out.TaskID] = deferCh
	defer delete(in.deferCh, out.TaskID)
	if in.deferEarly[out.TaskID] {
		deferCh.SendAsync(struct{}{})
	}

	sel := wf.NewSelector(ctx)
	var resolved, abandoned, deferred bool
	var sig stageTransitionSignal
	sel.AddReceive(resolveCh, func(c wf.ReceiveChannel, more bool) {
		c.Receive(ctx, &sig)
		resolved = true
	})
	sel.AddReceive(ctx.Done(), func(c wf.ReceiveChannel, more bool) {
		abandoned = true
	})
	sel.AddReceive(deferCh, func(c wf.ReceiveChannel, more bool) {
		c.Receive(ctx, nil)
		deferred = true
	})

	var fired *boundaryFire
	cancelBoundaries := registerTaskBoundaries(ctx, sel, in, string(nodeKey), stage.BoundaryTimer, stage.BoundaryMessage, func(f boundaryFire) {
		fired = &f
	})
	cancelSLA := addSLATimers(ctx, sel, slaTimerParams{
		TenantID: in.tenantID, InstanceID: in.instanceID, TaskID: out.TaskID, NodeKey: nodeKey,
		DueDate: stage.DueDate, FollowUpDate: stage.FollowUpDate,
	})

	for !resolved && fired == nil && !abandoned && !deferred {
		sel.Select(ctx)
	}
	cancelBoundaries()
	cancelPaths := func() {}
	if fired != nil && !fired.Interrupting && !abandoned && !deferred {
		var pathCtx wf.Context
		pathCtx, cancelPaths = wf.WithCancel(ctx)
		in.spawnFiredPath(pathCtx, plan, fired, string(nodeKey))
		for !resolved && !abandoned && !deferred {
			sel.Select(ctx)
		}
	}
	cancelSLA()

	if abandoned {
		in.supersedeAbandoned(ctx, out.TaskID)
		return nodeKey, "", errStageAbandoned
	}

	if deferred {
		cancelPaths()
		return nodeKey, "", errStageDeferred
	}

	if fired != nil && fired.Interrupting {
		delete(in.pending, visitKey)
		if err := updateTaskStatus(ctx, port.UpdateTaskStatusInput{
			TaskID: out.TaskID, TenantID: in.tenantID, Status: domain.TaskStatusSuperseded,
		}); err != nil {
			if abandonedMidActivity(ctx) {
				in.supersedeTask(ctx, out.TaskID)
				return nodeKey, "", errStageAbandoned
			}
			return nodeKey, "", err
		}
		releaseOpenStage()
		return in.runFiredPath(ctx, plan, fired)
	}

	return in.resolveTaskStage(ctx, nodeKey, out, sig)
}

// settleCreatedTask ends a stage cancelled while its task was being created.
// A create that still returned a task is superseded here even during a
// force-route: the route's audit only supersedes the tasks it knew about.
func (in *interpreter) settleCreatedTask(ctx wf.Context, out port.CreateTaskOutput, err error) error {
	if abandonedMidActivity(ctx) {
		if err == nil && out.TaskID != "" {
			in.supersedeTask(ctx, out.TaskID)
		}
		return errStageAbandoned
	}
	return err
}

func abandonedMidActivity(ctx wf.Context) bool {
	return ctx.Err() != nil
}

func (in *interpreter) supersedeAbandoned(ctx wf.Context, taskID string) {
	if in.routing {
		return
	}
	in.supersedeTask(ctx, taskID)
}

// supersedeTask marks the task SUPERSEDED on a context that outlives the
// cancelled run. A refused write is logged: the stage is abandoned either way.
func (in *interpreter) supersedeTask(ctx wf.Context, taskID string) {
	dctx, _ := wf.NewDisconnectedContext(ctx)
	if err := updateTaskStatus(dctx, port.UpdateTaskStatusInput{
		TaskID: taskID, TenantID: in.tenantID, Status: domain.TaskStatusSuperseded,
	}); err != nil {
		wf.GetLogger(ctx).Warn("abandoned stage: superseding its task failed", "task_id", taskID, "error", err)
	}
}

type stageFailedError struct {
	NodeKey domain.NodeKey
	Reason  string
}

func (e *stageFailedError) Error() string {
	return fmt.Sprintf("workflow: stage %q failed: %s", e.NodeKey, e.Reason)
}

func (in *interpreter) resolveTaskStage(
	ctx wf.Context, nodeKey domain.NodeKey, out port.CreateTaskOutput, sig stageTransitionSignal,
) (domain.NodeKey, string, error) {
	if sig.Failed {
		if err := updateTaskStatus(ctx, port.UpdateTaskStatusInput{
			TaskID: out.TaskID, TenantID: in.tenantID, Status: domain.TaskStatusFailed, RecordVersion: sig.RecordVersion,
			ErrorClass: sig.Reason,
		}); err != nil {
			return nodeKey, "", err
		}
		return nodeKey, "", &stageFailedError{NodeKey: nodeKey, Reason: sig.Reason}
	}

	completion, err := completeAssignment(ctx, port.CompleteAssignmentInput{
		TaskID: out.TaskID, UserID: sig.UserID, TenantID: in.tenantID, ResultJSON: sig.ResultJSON, RecordVersion: sig.RecordVersion,
	})
	if err != nil {
		return nodeKey, "", err
	}
	if completion.AllDone {
		if err := updateTaskStatus(ctx, port.UpdateTaskStatusInput{
			TaskID: out.TaskID, TenantID: in.tenantID, Status: domain.TaskStatusCompleted,
		}); err != nil {
			return nodeKey, "", err
		}
	}
	if err := in.mergeResult(sig.ResultJSON); err != nil {
		return nodeKey, "", err
	}
	in.history.Push(nodeKey)
	return nodeKey, sig.ResultJSON, nil
}
