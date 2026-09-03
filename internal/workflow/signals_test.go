package workflow

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func TestValidateSignal(t *testing.T) {
	tests := []struct {
		name    string
		status  domain.InstanceStatus
		signal  string
		wantErr bool
	}{
		{name: "stage-transition while running is allowed", status: domain.InstanceStatusRunning, signal: SignalStageTransition, wantErr: false},
		{name: "stage-transition while DEGRADED is allowed (a respawned or never-failed branch's own task)", status: domain.InstanceStatusDegraded, signal: SignalStageTransition, wantErr: false},
		{name: "stage-transition while paused is rejected", status: domain.InstanceStatusPaused, signal: SignalStageTransition, wantErr: true},
		{name: "instance-pause while running is allowed", status: domain.InstanceStatusRunning, signal: SignalInstancePause, wantErr: false},
		{
			// This is LLD §7.2 test #5's unit-level mechanism: a DEGRADED
			// instance rejects instance-pause at signal validation, before
			// the DEGRADED park loop's Selector — which registers no case
			// for instance-pause at all — ever sees it.
			name:    "instance-pause while DEGRADED is rejected",
			status:  domain.InstanceStatusDegraded,
			signal:  SignalInstancePause,
			wantErr: true,
		},
		{name: "instance-resume while paused is allowed", status: domain.InstanceStatusPaused, signal: SignalInstanceResume, wantErr: false},
		{name: "instance-resume while running is rejected", status: domain.InstanceStatusRunning, signal: SignalInstanceResume, wantErr: true},
		{name: "instance-cancel while running is allowed", status: domain.InstanceStatusRunning, signal: SignalInstanceCancel, wantErr: false},
		{name: "instance-cancel while paused is allowed", status: domain.InstanceStatusPaused, signal: SignalInstanceCancel, wantErr: false},
		{name: "instance-cancel while DEGRADED is allowed", status: domain.InstanceStatusDegraded, signal: SignalInstanceCancel, wantErr: false},
		{name: "instance-cancel while completed is rejected", status: domain.InstanceStatusCompleted, signal: SignalInstanceCancel, wantErr: true},
		{name: "instance-force-forward while DEGRADED is allowed", status: domain.InstanceStatusDegraded, signal: SignalInstanceForceFwd, wantErr: false},
		{name: "instance-force-forward while paused is rejected", status: domain.InstanceStatusPaused, signal: SignalInstanceForceFwd, wantErr: true},
		{name: "instance-force-back while running is allowed", status: domain.InstanceStatusRunning, signal: SignalInstanceForceBack, wantErr: false},
		{name: "unknown signal is rejected", status: domain.InstanceStatusRunning, signal: "not-a-real-signal", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSignal(tt.status, tt.signal)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSignal(%s, %s) error = %v, wantErr %v", tt.status, tt.signal, err, tt.wantErr)
			}
		})
	}
}

// TestStageDeferOnPendingStageDoesNotWipeHistory is regression coverage for
// a bug where SignalStageDefer called history.PopTo on the currently-pending
// stage being deferred — a node never yet in history (history.Push only
// happens once a stage completes, stage.go) — so PopTo's own not-found
// fallback wiped the entire stack, including an unrelated, already-completed
// earlier stage. Deferring the second (pending) stage must leave the first
// (already-completed) stage's history entry intact.
func TestStageDeferOnPendingStageDoesNotWipeHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceNodesInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			return port.CompleteAssignmentOutput{AllDone: true}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.DeferTaskInput) (port.DeferTaskOutput, error) {
			return port.DeferTaskOutput{}, nil
		},
		activity.RegisterOptions{Name: port.ActivityDeferTask},
	)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "deptA", ToStage: "prep", ResultJSON: "{}"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-defer:instance", stageDeferSignal{DeptID: "deptA", FromStage: "approve", Reason: "need more info", UserID: "user-1"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "deptA", ToStage: "approve", ResultJSON: "{}"})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name: "main",
			Departments: []dsl.DepartmentDef{{
				ID:     "deptA",
				Stages: []dsl.StageDef{{Type: "prep"}, {Type: "approve"}},
			}},
		}
		_, err := in.runDepartmentFrom(ctx, plan, "deptA", 0)
		if err != nil {
			return err
		}
		if len(in.history.stack) != 2 {
			return errBadHistoryLen(len(in.history.stack))
		}
		if in.history.stack[0] != "deptA/prep" || in.history.stack[1] != "deptA/approve" {
			return errBadHistoryLen(-3)
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}
