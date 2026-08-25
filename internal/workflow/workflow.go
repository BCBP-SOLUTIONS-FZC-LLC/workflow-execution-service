// Package workflow is the Temporal workflow-function interpreter for
// execution_service's compiled BPMN DSL. It is a peer of internal/core/service,
// not internal/adapter: never imported by adapter/, never imports it back,
// wired into cmd/worker only via Temporal's runtime registration.
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

	if err := in.registerStatusQuery(ctx); err != nil {
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, err
	}

	admin := wf.NewBufferedChannel(ctx, 8)
	baseAdmin := wf.NewBufferedChannel(ctx, 8)
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
	// A Terminated outcome already went through cancelInstance (at whichever
	// of runTopLevel/runParallel/enterDegraded received the signal), which
	// writes TERMINATED + the task-FAILED cascade + both event classes in one
	// Activity — calling the generic update again here would be a redundant,
	// spurious status-update event on top of an already-complete write.
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

// Listens on baseAdmin rather than admin (runParallel/enterDegraded's own
// channel) — the router sends each signal to exactly one, since two
// Selectors racing the same channel would make delivery ambiguous.
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
				return // force-forwarded away; doneCh has no listener left for this iteration
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
		cancelRun()

		if done {
			return outcome, runErr
		}

		sig := envelope.Signal
		switch envelope.Kind {
		case SignalInstanceCancel:
			return stepOutcome{Terminated: true}, in.cancelInstanceOnSignal(ctx, sig)

		case SignalInstanceForceFwd:
			// Unlike force-back, this must never call history.PopTo.
			var oldKeys []domain.NodeKey
			if last := in.history.Peek(); last != "" {
				oldKeys = []domain.NodeKey{last}
			}
			in.recordAndRedirect(ctx, oldKeys, sig)
			steps = redirectSteps(original, deptFromNodeKey(sig.TargetNodeKey))

		case SignalInstanceForceBack:
			popped := in.history.PopTo(sig.TargetNodeKey)
			in.msgBuf.ResetSpan(popped)
			steps = redirectSteps(original, deptFromNodeKey(sig.TargetNodeKey))
		}
	}
}

// Errors are deliberately swallowed: both already retry unlimited on any
// retryable failure, so an error here is non-retryable and best ignored
// rather than aborting the instance over a best-effort audit write.
func (in *interpreter) recordAndRedirect(ctx wf.Context, oldNodeKeys []domain.NodeKey, sig adminSignal) {
	_ = recordForceRoute(ctx, port.RecordForceRouteInput{
		InstanceID: in.instanceID, TenantID: in.tenantID,
		OldNodeKeys: oldNodeKeys, TargetNodeID: string(sig.TargetNodeKey),
		AdminUserID: sig.AdminUserID, RecordVersion: sig.RecordVersion,
	})
	_ = updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{sig.TargetNodeKey},
	})
}

func redirectSteps(original []dsl.ExecutionStep, deptID string) []dsl.ExecutionStep {
	for i, step := range original {
		for j, d := range step.Sequential {
			if d == deptID {
				remainder := append([]string{}, step.Sequential[j:]...)
				out := []dsl.ExecutionStep{{Sequential: remainder}}
				return append(out, original[i+1:]...)
			}
		}
	}
	return []dsl.ExecutionStep{{Sequential: []string{deptID}}}
}
