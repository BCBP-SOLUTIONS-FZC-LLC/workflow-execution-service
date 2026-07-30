// Package workflow is the Temporal workflow-function interpreter for
// execution_service's compiled BPMN DSL (workflow-models's pkg/dsl). It is a
// peer of internal/core/service, not internal/adapter (LLD §1.7): never
// imported by adapter/, never imports it back, wired into cmd/worker only
// via Temporal's runtime registration.
//
// No dsl_schema_version fail-closed check is implemented here (LLD
// §2.5/§3.1/§3.3): the field doesn't exist in workflow-models v1.0.0, and
// even once it does, a same-workflow version check is only ever a
// last-resort trip-wire, not a fix — a long-running instance would already
// be stuck interpreting a shape it can't handle by the time it fires. The
// actual compatibility strategy is Temporal's Worker Deployment Versions +
// Workflow Pinning: in-flight instances stay pinned to the Deployment
// Version they started on, new instances get the new one, old fleets drain
// naturally. That's a cmd/worker/deploy-layer concern (sibling tasks); this
// package only needs to stay deterministic and GetVersion-guarded
// (version.go).
package workflow

import (
	"fmt"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// Execute is the workflow function itself (LLD §2.5); registering it with
// worker.RegisterWorkflow is cmd/worker's job, a separate sibling task.
func Execute(ctx wf.Context, input ExecuteInput) (ExecuteOutput, error) {
	getVersion(ctx, initialInterpreterChangeID)

	planOut, err := getCompiledPlan(ctx, port.GetCompiledPlanInput{TenantID: input.TenantID, VersionID: input.VersionID})
	if err != nil {
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: input.InstanceID, TenantID: input.TenantID, Status: domain.InstanceStatusFailed,
		})
		return ExecuteOutput{Status: domain.InstanceStatusFailed}, err
	}

	in := newInterpreter(input.TenantID, input.InstanceID, input.ContextJSON, &planOut.Collaboration)

	mainPlan := findPlan(in.collab, in.collab.MainPlan)
	if mainPlan == nil {
		err := fmt.Errorf("workflow: main plan %q not found in compiled collaboration", in.collab.MainPlan)
		_ = updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
			InstanceID: in.instanceID, TenantID: in.tenantID, Status: domain.InstanceStatusFailed,
		})
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
	now := wf.Now(ctx)
	if err := updateInstanceStatus(ctx, port.UpdateInstanceStatusInput{
		InstanceID: in.instanceID, TenantID: in.tenantID, Status: finalStatus, CompletedAt: &now,
	}); err != nil && runErr == nil {
		runErr = err
	}

	return ExecuteOutput{Status: finalStatus}, runErr
}

// runTopLevel is the base (non-DEGRADED, no active Parallel gateway)
// force-back/force-forward/cancel handler: it races the plan's dispatch
// against baseAdmin and, on a redirect signal, cancels the in-flight
// dispatch and restarts at the target department (LLD §2.7's base
// mechanism). It listens on baseAdmin rather than admin — the channel
// runParallel/enterDegraded use — because signals.go's router sends each
// signal to exactly one of the two based on whether a Parallel gateway is
// currently active; two Selectors racing the same channel would make
// delivery ambiguous.
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
			return stepOutcome{Terminated: true}, nil

		case SignalInstanceForceFwd:
			// Unlike force-back, force-forward jumps to a node that hasn't
			// been visited yet — it must never call history.PopTo, which
			// pops the ENTIRE stack when the target isn't found in it
			// (that's force-back's regression semantics, not this one).
			// OldNodeKeys reports only the currently bypassed position.
			var oldKeys []domain.NodeKey
			if last := in.history.Peek(); last != "" {
				oldKeys = []domain.NodeKey{last}
			}
			// Errors from these two are deliberately not propagated: both
			// already retry unlimited on any retryable failure
			// (dbWriteActivityOptions), so an error here means a genuinely
			// non-retryable one (bad input, not-found) on an admin-issued
			// signal — surfacing it would abort the whole instance over a
			// best-effort audit/projection write, which is a worse outcome
			// than proceeding with the redirect the admin actually asked for.
			_ = recordForceRoute(ctx, port.RecordForceRouteInput{
				InstanceID: in.instanceID, TenantID: in.tenantID,
				OldNodeKeys: oldKeys, TargetNodeID: string(sig.TargetNodeKey),
				AdminUserID: sig.AdminUserID, RecordVersion: sig.RecordVersion,
			})
			_ = updateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
				InstanceID: in.instanceID, TenantID: in.tenantID, NodeKeys: []domain.NodeKey{sig.TargetNodeKey},
			})
			steps = redirectSteps(original, deptFromNodeKey(sig.TargetNodeKey))

		case SignalInstanceForceBack:
			popped := in.history.PopTo(sig.TargetNodeKey)
			in.msgBuf.ResetSpan(popped)
			steps = redirectSteps(original, deptFromNodeKey(sig.TargetNodeKey))
		}
	}
}

// redirectSteps resumes at deptID by finding it within original (the
// plan's own top-level steps — never an already-redirected list, so
// repeated redirects don't compound) and keeping every sibling department
// and step after it, so the rest of the plan still runs once deptID
// completes again. deptID not found here (e.g. nested inside a
// SubWorkflow/Parallel) falls back to running it in isolation — the DSL has
// no finer-grained continuation pointer than dept+stage+nodeID.
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
