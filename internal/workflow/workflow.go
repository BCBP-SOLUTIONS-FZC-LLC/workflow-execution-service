package workflow

import (
	"errors"
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func Execute(ctx wf.Context, input ExecuteInput) (ExecuteOutput, error) {
	getVersion(ctx, initialInterpreterChangeID)

	planOut, err := getCompiledPlan(ctx, port.GetCompiledPlanInput{TenantID: input.TenantID, VersionID: input.VersionID})
	if err != nil {
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: input.InstanceID, TenantID: input.TenantID, Status: domain.InstanceStatusFailed,
		})
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, err
	}

	strategy, ok := resolveSchemaStrategy(planOut.Collaboration.SchemaVersion)
	if !ok {
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: input.InstanceID, TenantID: input.TenantID, Status: domain.InstanceStatusFailed,
		})
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, fmt.Errorf("workflow: unsupported dsl schema version %d", planOut.Collaboration.SchemaVersion)
	}
	collab := strategy.normalize(&planOut.Collaboration)

	in := newInterpreter(input.TenantID, input.InstanceID, input.ContextJSON, collab, input.OverrideMap)

	mainPlan := findPlan(in.collab, in.collab.MainPlan)
	if mainPlan == nil {
		err := fmt.Errorf("workflow: main plan %q not found in compiled collaboration", in.collab.MainPlan)
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusFailed,
		})
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, err
	}

	if err := in.registerWaitingExternalQuery(ctx); err != nil {
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusFailed,
		})
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, err
	}

	if err := in.registerStatusQuery(ctx); err != nil {
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusFailed,
		})
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, err
	}

	admin := wf.NewBufferedChannel(ctx, 8)
	in.admin = admin
	baseAdmin := wf.NewBufferedChannel(ctx, 8)
	in.baseAdmin = baseAdmin
	wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

	outcome, runErr := in.runTopLevel(ctx, mainPlan, admin, baseAdmin)

	finalStatus := domain.InstanceStatusCompleted
	switch {
	case runErr != nil:
		finalStatus = domain.InstanceStatusFailed
	case outcome.Terminated:
		finalStatus = domain.InstanceStatusTerminated
	}
	in.status = finalStatus
	if finalStatus != domain.InstanceStatusTerminated {
		now := wf.Now(ctx)
		if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: in.instanceID, TenantID: in.tenantID, Status: finalStatus, CompletedAt: &now,
		}); err != nil && runErr == nil {
			runErr = err
		}
	}

	return ExecuteOutput{Status: finalStatus}, runErr
}

func (in *interpreter) runTopLevel(ctx wf.Context, plan *dsl.CompiledPlan, admin, baseAdmin wf.Channel) (stepOutcome, error) {
	original := plan.Execution.Steps
	steps := original
	for {
		runCtx, cancelRun := wf.WithCancel(ctx)
		doneCh := wf.NewChannel(ctx)
		var outcome stepOutcome
		var runErr error
		wf.Go(runCtx, func(gctx wf.Context) {
			outcome, runErr = in.runSteps(gctx, plan, steps, admin)
			if errors.Is(runErr, errStageAbandoned) {
				return
			}
			doneCh.Send(gctx, struct{}{})
		})

		sel := wf.NewSelector(ctx)
		done := false
		sel.AddReceive(doneCh, func(c wf.ReceiveChannel, more bool) {
			var v struct{}
			c.Receive(ctx, &v)
			done = true
		})
		var envelope adminSignalEnvelope
		sel.AddReceive(baseAdmin, func(c wf.ReceiveChannel, more bool) {
			c.Receive(ctx, &envelope)
		})
		sel.Select(ctx)
		abandonedNodes := in.currentPendingNodes()
		in.routing = envelope.Kind != ""
		cancelRun()

		if done {
			return outcome, runErr
		}

		sig := envelope.Signal
		switch envelope.Kind {
		case SignalInstanceCancel:
			return stepOutcome{Terminated: true}, in.cancelInstanceOnSignal(ctx, sig)

		case SignalInstanceForceFwd:
			in.recordAndRedirect(ctx, abandonedNodes, sig.TargetNodeKey, domain.ForceRouteDirectionForward, sig)
			steps = redirectSteps(original, deptFromNodeKey(sig.TargetNodeKey))

		case SignalInstanceForceBack:
			target, popped, ok := in.history.PopOne()
			if !ok {
				wf.GetLogger(ctx).Warn("instance-force-back: no completed node to return to; dropping")
				in.routing = false
				continue
			}
			in.msgBuf.ResetSpan(popped)
			in.recordAndRedirect(ctx, abandonedNodes, target, domain.ForceRouteDirectionBack, sig)
			steps = redirectSteps(original, deptFromNodeKey(target))
		}
		in.routing = false
	}
}

func (in *interpreter) recordAndRedirect(ctx wf.Context, oldNodeKeys []domain.NodeKey, target domain.NodeKey, direction string, sig adminSignal) {
	if !sig.FromDefer {
		in.recordForceRouteAudit(ctx, oldNodeKeys, target, direction, sig)
	}
	_ = updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{target},
	})
}

func (in *interpreter) recordForceRouteAudit(ctx wf.Context, oldNodeKeys []domain.NodeKey, target domain.NodeKey, direction string, sig adminSignal) {
	_ = recordForceRoute(ctx, port.RecordForceRouteInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		OldNodeKeys: oldNodeKeys, TargetNodeID: string(target), Direction: direction,
		AdminUserID: sig.AdminUserID, RecordVersion: sig.RecordVersion,
	})
}

func redirectSteps(original []dsl.ExecutionStep, deptID string) []dsl.ExecutionStep {
	if cont, ok := findRedirectTarget(original, deptID); ok {
		return cont
	}
	return []dsl.ExecutionStep{{Sequential: []string{deptID}}}
}

func findRedirectTarget(steps []dsl.ExecutionStep, deptID string) ([]dsl.ExecutionStep, bool) {
	for i := range steps {
		if cont, ok := redirectWithinStep(&steps[i], deptID); ok {
			return append(cont, steps[i+1:]...), true
		}
	}
	return nil, false
}

func redirectWithinStep(step *dsl.ExecutionStep, deptID string) ([]dsl.ExecutionStep, bool) {
	for j, d := range step.Sequential {
		if d == deptID {
			remainder := append([]string{}, step.Sequential[j:]...)
			return []dsl.ExecutionStep{{Sequential: remainder}}, true
		}
	}
	for _, b := range step.Parallel {
		if cont, ok := findRedirectTarget(b.Steps, deptID); ok {
			return cont, true
		}
		if b.DeptID == deptID {
			return b.Steps, true
		}
	}
	for _, b := range step.Exclusive {
		if cont, ok := findRedirectTarget(b.Steps, deptID); ok {
			return cont, true
		}
		if len(b.Steps) == 0 && (b.Target == deptID || b.RevertToDept == deptID) {
			return []dsl.ExecutionStep{{Sequential: []string{deptID}}}, true
		}
	}
	if step.SubWorkflow != nil {
		if cont, ok := findRedirectTarget(step.SubWorkflow.Plan.Steps, deptID); ok {
			return cont, true
		}
	}
	return nil, false
}
