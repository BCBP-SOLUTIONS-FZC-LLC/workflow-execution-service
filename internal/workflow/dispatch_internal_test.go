package workflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func newTestEnv() *testsuite.TestWorkflowEnvironment {
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
		func(_ context.Context, _ port.UpdateTaskStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
	return env
}

// TestRunStageSendReceiveTask exercises runStage's send_task/receive_task
// branches directly.
func TestRunStageSendReceiveTask(t *testing.T) {
	env := newTestEnv()

	var sendKey, recvKey domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		plan := &dsl.CompiledPlan{Name: "main"}

		k, err := in.runStage(ctx, plan, "sales", &dsl.StageDef{Type: "send_task", Extras: map[string]string{"message": "m"}})
		if err != nil {
			return err
		}
		sendKey = k

		k2, err := in.runStage(ctx, plan, "shipping", &dsl.StageDef{Type: "receive_task", Extras: map[string]string{"message": "m"}})
		if err != nil {
			return err
		}
		recvKey = k2
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if sendKey != "sales/send_task" {
		t.Errorf("send_task nodeKey = %v, want sales/send_task", sendKey)
	}
	if recvKey != "shipping/receive_task" {
		t.Errorf("receive_task nodeKey = %v, want shipping/receive_task", recvKey)
	}
}

// TestRunTaskStage_AbandonedOnCancel exercises the fix for a branch parked
// in runTaskStage's own Selector: canceling ctx (a force-forward past it)
// must return errStageAbandoned promptly, not hang forever.
func TestRunTaskStage_AbandonedOnCancel(t *testing.T) {
	env := newTestEnv()

	var stageErr error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		plan := &dsl.CompiledPlan{Name: "main"}

		cctx, cancel := wf.WithCancel(ctx)
		wf.Go(ctx, func(gctx wf.Context) {
			_ = wf.Sleep(gctx, time.Millisecond)
			cancel()
		})

		_, stageErr = in.runTaskStage(cctx, plan, "sales", &dsl.StageDef{Type: "approve", NodeID: "n1"}, "sales/n1")
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !errors.Is(stageErr, errStageAbandoned) {
		t.Errorf("runTaskStage error = %v, want errStageAbandoned", stageErr)
	}
}

// TestRunStageLogsEngineNoteForUnrecognizedType exercises the EngineNote
// logging branch for an unrecognized stage Type — forward-compat
// passthrough, never a failure (LLD §2.4).
func TestRunStageLogsEngineNoteForUnrecognizedType(t *testing.T) {
	env := newTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "sales", ToStage: "custom_role", ResultJSON: "{}"})
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })
		plan := &dsl.CompiledPlan{Name: "main"}
		_, err := in.runStage(ctx, plan, "sales", &dsl.StageDef{
			Type: "custom_role", Activity: "act",
			EngineNote: "stage type 'custom_role' is not a defined class in the workflow engine",
		})
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// TestRunExclusiveTerminates exercises runExclusive's Terminates:true path.
func TestRunExclusiveTerminates(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var terminated bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.lastResultJSON = `{"decision":"reject"}`
		plan := &dsl.CompiledPlan{Name: "main"}
		out, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `decision == "reject"`, Terminates: true},
		})
		terminated = out.Terminated
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !terminated {
		t.Error("runExclusive should report Terminated:true")
	}
}

// TestRunExclusiveRevertPopsHistory exercises runExclusive's RevertTo* path
// — structurally the same transfer mechanism as force-back, but
// condition-triggered (LLD §2.6 point 4).
func TestRunExclusiveRevertPopsHistory(t *testing.T) {
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

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "rework", ToStage: "prep", ResultJSON: "{}"})
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", `{"decision":"rejected"}`, nil, nil)
		in.lastResultJSON = `{"decision":"rejected"}`
		in.history.Push("rework/prep")
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: "rework", Stages: []dsl.StageDef{{Type: "prep"}}}},
		}
		_, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `decision == "rejected"`, RevertToDept: "rework", RevertToStage: "prep"},
		})
		if len(in.history.stack) != 1 {
			return errBadHistoryLen(len(in.history.stack))
		}
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// TestRunExclusiveForwardFallsBackToTargetFromTop exercises runExclusive's
// plain Target dispatch when neither TargetNodeID nor TargetStage is set —
// the genuinely non-ambiguous case (LLD §2.6's field table: "Dispatch uses
// TargetNodeID/RevertToNodeID when present, falling back to
// Target+TargetStage, then bare Target otherwise").
func TestRunExclusiveForwardFallsBackToTargetFromTop(t *testing.T) {
	env := newTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "shipping", NodeID: "Task_ship", ResultJSON: "{}"})
	}, time.Millisecond)

	var lastNode domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.lastResultJSON = `{"decision":"approved"}`
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: "shipping", Stages: []dsl.StageDef{{Type: "approve", NodeID: "Task_ship"}}}},
		}
		out, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `decision == "approved"`, Target: "shipping"},
		})
		lastNode = out.LastNode
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if lastNode != "shipping/Task_ship" {
		t.Errorf("LastNode = %q, want shipping/Task_ship", lastNode)
	}
}

// TestRunExclusiveUsesTargetStageToAvoidReRunningEarlierStage exercises the
// TargetStage forward path (LLD §2.6) for a department whose stages carry no
// NodeID — TargetStage must resolve to the matching stage.Type's own index,
// not restart the department from Stages[0].
func TestRunExclusiveUsesTargetStageToAvoidReRunningEarlierStage(t *testing.T) {
	env := newTestEnv()

	var createdNodeKeys []domain.NodeKey
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			createdNodeKeys = append(createdNodeKeys, in.NodeKey)
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

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "billing", ToStage: "invoice", ResultJSON: "{}"})
	}, time.Millisecond)

	var lastNode domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.lastResultJSON = `{"decision":"approved"}`
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name: "main",
			Departments: []dsl.DepartmentDef{{
				ID: "billing",
				Stages: []dsl.StageDef{
					{Type: "collect"},
					{Type: "invoice"},
				},
			}},
		}
		out, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `decision == "approved"`, Target: "billing", TargetStage: "invoice"},
		})
		lastNode = out.LastNode
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if lastNode != "billing/invoice" {
		t.Errorf("LastNode = %q, want billing/invoice", lastNode)
	}
	if len(createdNodeKeys) != 1 || createdNodeKeys[0] != "billing/invoice" {
		t.Errorf("createdNodeKeys = %v, want exactly [billing/invoice] — TargetStage should dispatch straight to billing/invoice, not restart department \"billing\" from Stages[0] and re-run billing/collect", createdNodeKeys)
	}
}

// TestRunExclusiveUsesTargetNodeIDToAvoidReRunningEarlierStage exercises the
// TargetNodeID forward path (LLD §2.6) with a fixture mirroring
// definition_service's TestCompile_SendTaskInExclusiveBranch shape: a
// send_task branch landing in the same department ("design") as the prep
// stage that ran immediately before the gateway. Target+TargetStage alone
// can't express "just this branch's stage" here — dispatch must resolve
// TargetNodeID to design's Stages[1], not restart design from Stages[0] and
// re-run the prep stage.
func TestRunExclusiveUsesTargetNodeIDToAvoidReRunningEarlierStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var createdNodeKeys []domain.NodeKey
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			createdNodeKeys = append(createdNodeKeys, in.NodeKey)
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

	// Safety net only: if a regression reintroduces the index-0 restart,
	// this lets the buggy run resolve Task_prep instead of hanging — the
	// real assertion is on createdNodeKeys below, not on this firing.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "design", NodeID: "Task_prep", ResultJSON: "{}"})
	}, time.Millisecond)

	var lastNode domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.lastResultJSON = `{"notify":"true"}`
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name: "main",
			Departments: []dsl.DepartmentDef{{
				ID: "design",
				Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Task_prep"},
					{Type: "send_task", NodeID: "Task_send", Extras: map[string]string{"message": "notify"}},
				},
			}},
		}
		out, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `notify == "true"`, Target: "design", TargetStage: "send_task", TargetNodeID: "Task_send"},
		})
		lastNode = out.LastNode
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if lastNode != "design/Task_send" {
		t.Errorf("LastNode = %q, want design/Task_send", lastNode)
	}
	if len(createdNodeKeys) != 0 {
		t.Errorf("CreateTaskActivity called for %v, want no calls — TargetNodeID should dispatch straight to design/Task_send (a send_task, no CreateTask involved), not restart department \"design\" from Stages[0] and re-run design/Task_prep", createdNodeKeys)
	}
}

// TestRunExclusiveRevertUsesRevertToNodeID exercises the RevertToNodeID
// revert path with the same "shares a department with an earlier stage"
// shape as the forward test above: a back-edge into department "rework"'s
// second stage must resolve via RevertToNodeID to Stages[1], not restart
// "rework" from Stages[0] and re-run Task_prep.
func TestRunExclusiveRevertUsesRevertToNodeID(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var createdNodeKeys []domain.NodeKey
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			createdNodeKeys = append(createdNodeKeys, in.NodeKey)
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

	// Safety net only, same rationale as the forward test above.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "rework", NodeID: "Task_prep", ResultJSON: "{}"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "rework", NodeID: "Task_approve", ResultJSON: "{}"})
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.lastResultJSON = `{"decision":"rejected"}`
		in.history.Push("rework/Task_prep")
		in.history.Push("rework/Task_approve")
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name: "main",
			Departments: []dsl.DepartmentDef{{
				ID: "rework",
				Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Task_prep"},
					{Type: "approve", NodeID: "Task_approve"},
				},
			}},
		}
		out, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `decision == "rejected"`, RevertToDept: "rework", RevertToStage: "approve", RevertToNodeID: "Task_approve"},
		})
		if err != nil {
			return err
		}
		if out.LastNode != "rework/Task_approve" {
			return errBadHistoryLen(-2)
		}
		if len(in.history.stack) != 2 {
			return errBadHistoryLen(len(in.history.stack))
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if len(createdNodeKeys) != 1 || createdNodeKeys[0] != "rework/Task_approve" {
		t.Errorf("createdNodeKeys = %v, want exactly [rework/Task_approve] — RevertToNodeID should skip straight to Task_approve, not restart department \"rework\" from Stages[0] and re-run Task_prep", createdNodeKeys)
	}
}

// TestRunExclusiveRevertUsesRevertToStage exercises the RevertToStage revert
// path for a department whose stages carry no NodeID — same "shares a
// department with an earlier stage" shape as the RevertToNodeID test above,
// but resolved via stage.Type instead.
func TestRunExclusiveRevertUsesRevertToStage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var createdNodeKeys []domain.NodeKey
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			createdNodeKeys = append(createdNodeKeys, in.NodeKey)
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

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "rework", ToStage: "prep", ResultJSON: "{}"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "rework", ToStage: "approve", ResultJSON: "{}"})
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.lastResultJSON = `{"decision":"rejected"}`
		in.history.Push("rework/prep")
		in.history.Push("rework/approve")
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name: "main",
			Departments: []dsl.DepartmentDef{{
				ID: "rework",
				Stages: []dsl.StageDef{
					{Type: "prep"},
					{Type: "approve"},
				},
			}},
		}
		out, err := in.runExclusive(ctx, plan, []dsl.ExclusiveBranch{
			{ConditionExpression: `decision == "rejected"`, RevertToDept: "rework", RevertToStage: "approve"},
		})
		if err != nil {
			return err
		}
		if out.LastNode != "rework/approve" {
			return errBadHistoryLen(-2)
		}
		if len(in.history.stack) != 2 {
			return errBadHistoryLen(len(in.history.stack))
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if len(createdNodeKeys) != 1 || createdNodeKeys[0] != "rework/approve" {
		t.Errorf("createdNodeKeys = %v, want exactly [rework/approve] — RevertToStage should skip straight to approve, not restart department \"rework\" from Stages[0] and re-run prep", createdNodeKeys)
	}
}

type errBadHistoryLen int

func (e errBadHistoryLen) Error() string { return "unexpected history length" }

func TestFindDepartmentNotFound(t *testing.T) {
	plan := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{{ID: "a"}}}
	if got := findDepartment(plan, "does-not-exist"); got != nil {
		t.Errorf("findDepartment() = %+v, want nil", got)
	}
}

func TestFindPlanNotFound(t *testing.T) {
	collab := &dsl.CompiledCollaboration{Plans: []*dsl.CompiledPlan{{Name: "a"}}}
	if got := findPlan(collab, "does-not-exist"); got != nil {
		t.Errorf("findPlan() = %+v, want nil", got)
	}
}

func TestDeptFromNodeKeyWithoutColon(t *testing.T) {
	if got := deptFromNodeKey("no-colon-here"); got != "no-colon-here" {
		t.Errorf("deptFromNodeKey() = %q, want no-colon-here", got)
	}
}

func TestStageIndexAfterNotFound(t *testing.T) {
	dept := &dsl.DepartmentDef{ID: "a", Stages: []dsl.StageDef{{Type: "prep"}}}
	if got := stageIndexAfter(dept, "a:does-not-exist"); got != 0 {
		t.Errorf("stageIndexAfter() = %d, want 0", got)
	}
}

func TestIndexOfFailedBranchNotFound(t *testing.T) {
	if got := indexOfFailedBranch([]failedBranch{{DeptID: "a"}}, "b"); got != -1 {
		t.Errorf("indexOfFailedBranch() = %d, want -1", got)
	}
}

// TestRunStepsUnpopulatedVariantErrors exercises runSteps' default branch:
// an ExecutionStep with no variant field populated at all is a compiled-plan
// invariant violation, not a silently-skipped no-op.
func TestRunStepsUnpopulatedVariantErrors(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		plan := &dsl.CompiledPlan{Name: "main"}
		_, err := in.runSteps(ctx, plan, []dsl.ExecutionStep{{}}, admin)
		if err == nil {
			return errBadHistoryLen(-1)
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// TestRunStepsSequentialPreservesLastNodeOnLaterFailure is regression
// coverage for a bug where a Sequential list's second department failing
// overwrote `last` with its own (empty, since its first stage never
// completed) return value — losing the first department's real completion.
// Callers (DEGRADED respawn, RecordForceRoute's audit trail) depend on
// LastNode surviving a later sibling's failure.
func TestRunStepsSequentialPreservesLastNodeOnLaterFailure(t *testing.T) {
	env := newTestEnv()
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			if in.NodeKey == "dept2/prep" {
				return port.CreateTaskOutput{}, errors.New("dept2 failed")
			}
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "dept1", ToStage: "prep", ResultJSON: "{}"})
	}, time.Millisecond)

	var lastNode domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "dept1", Stages: []dsl.StageDef{{Type: "prep"}}},
				{ID: "dept2", Stages: []dsl.StageDef{{Type: "prep"}}},
			},
		}
		out, err := in.runSteps(ctx, plan, []dsl.ExecutionStep{{Sequential: []string{"dept1", "dept2"}}}, admin)
		lastNode = out.LastNode
		if err == nil {
			return errBadHistoryLen(-6)
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if lastNode != "dept1/prep" {
		t.Errorf("LastNode = %q, want dept1/prep — dept2's failure must not overwrite dept1's real completion", lastNode)
	}
}

// TestEnterDegradedUnknownTargetDeptIsANoOp exercises enterDegraded's
// idx<0 early-return branches: a force-forward/force-back naming a
// department that isn't actually failed is dropped, not misapplied to the
// wrong branch.
func TestEnterDegradedUnknownTargetDeptIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.RecordForceRouteInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityRecordForceRoute},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceNodesInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance", adminSignal{TargetDeptID: "not-the-failed-one"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance", adminSignal{TargetDeptID: "not-the-failed-one"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance", adminSignal{TargetDeptID: "billing", TargetNodeKey: "billing/prep"})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })
		in.parallelDepth++

		plan := &dsl.CompiledPlan{Name: "main"}
		_, err := in.enterDegraded(ctx, plan, "", nil, []failedBranch{{DeptID: "billing", LastCompletedNode: "billing/prep"}}, admin)
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// TestExecuteFailsWhenCompiledPlanFetchErrors exercises Execute's earliest
// error path: GetCompiledPlanActivity itself failing.
func TestExecuteFailsWhenCompiledPlanFetchErrors(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			return port.GetCompiledPlanOutput{}, errors.New("definition service unavailable")
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)

	env.ExecuteWorkflow(Execute, ExecuteInput{TenantID: "t", InstanceID: "i", VersionID: "v"})

	if env.GetWorkflowError() == nil {
		t.Fatal("Execute should fail when GetCompiledPlanActivity errors")
	}
}

// TestExecuteFailsWhenMainPlanMissing exercises Execute's second error
// path: a CompiledCollaboration whose MainPlan doesn't resolve.
func TestExecuteFailsWhenMainPlanMissing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			return port.GetCompiledPlanOutput{Collaboration: dsl.CompiledCollaboration{MainPlan: "missing", SchemaVersion: dsl.CurrentSchemaVersion}}, nil
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)

	env.ExecuteWorkflow(Execute, ExecuteInput{TenantID: "t", InstanceID: "i", VersionID: "v"})

	if env.GetWorkflowError() == nil {
		t.Fatal("Execute should fail when the main plan can't be resolved")
	}
}

// TestRunTaskStageInterruptingBoundaryTransfers exercises runTaskStage's
// fired-and-interrupting path directly: the in-flight task is abandoned and
// control transfers to the boundary's TargetDept (LLD §2.2 step 5) — and the
// abandoned task is marked SUPERSEDED rather than left open forever with no
// terminal status.
func TestRunTaskStageInterruptingBoundaryTransfers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
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

	var supersededCalls []port.UpdateTaskStatusInput
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateTaskStatusInput) error {
			supersededCalls = append(supersededCalls, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "escalation", ToStage: "prep", ResultJSON: "{}"})
	}, 2*time.Hour)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: "escalation", Stages: []dsl.StageDef{{Type: "prep"}}}},
		}
		stage := &dsl.StageDef{
			Type: "approve", NodeID: "n1",
			BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "escalation"},
		}
		_, err := in.runTaskStage(ctx, plan, "sales", stage, "sales/n1")
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if len(supersededCalls) != 1 || supersededCalls[0].TaskID != "sales/n1" || supersededCalls[0].Status != domain.TaskStatusSuperseded {
		t.Errorf("UpdateTaskStatusActivity calls = %+v, want exactly one SUPERSEDED call for sales/n1", supersededCalls)
	}
}

// TestRunTaskStageNonInterruptingBoundaryContinuesBoth exercises
// runTaskStage's fired-and-non-interrupting path: the host keeps running
// and completes normally while the boundary target also proceeds
// independently (LLD §2.2 step 5).
func TestRunTaskStageNonInterruptingBoundaryContinuesBoth(t *testing.T) {
	env := newTestEnv()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "sales", ToStage: "approve", NodeID: "n1", ResultJSON: "{}"})
	}, 2*time.Hour)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		plan := &dsl.CompiledPlan{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: "escalation", Stages: []dsl.StageDef{{Type: "prep"}}}},
		}
		stage := &dsl.StageDef{
			Type: "approve", NodeID: "n1",
			BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: false, TargetDept: "escalation"},
		}
		_, err := in.runTaskStage(ctx, plan, "sales", stage, "sales/n1")
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// TestRedirectStepsSequential is regression coverage for the pre-existing,
// already-correct case: a deptID found directly under a top-level Sequential
// step resumes from there, with every later top-level step preserved.
func TestRedirectStepsSequential(t *testing.T) {
	original := []dsl.ExecutionStep{
		{Sequential: []string{"intake", "review"}},
		{Sequential: []string{"shipping"}},
	}
	got := redirectSteps(original, "review")
	want := []dsl.ExecutionStep{
		{Sequential: []string{"review"}},
		{Sequential: []string{"shipping"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("redirectSteps() = %+v, want %+v", got, want)
	}
}

// TestRedirectStepsParallelBranch is the regression case for the original
// data-loss bug: deptID lives inside a ParallelBranch, not a top-level
// Sequential step. redirectSteps must resolve to that branch's own Steps
// and must NOT drop the Sequential step that was still queued after the
// Parallel step.
func TestRedirectStepsParallelBranch(t *testing.T) {
	original := []dsl.ExecutionStep{
		{Parallel: []dsl.ParallelBranch{
			{DeptID: "deptA", Steps: []dsl.ExecutionStep{{Sequential: []string{"deptA"}}}},
			{DeptID: "deptB", Steps: []dsl.ExecutionStep{{Sequential: []string{"deptB"}}}},
		}},
		{Sequential: []string{"shipping"}},
	}
	got := redirectSteps(original, "deptA")
	want := []dsl.ExecutionStep{
		{Sequential: []string{"deptA"}},
		{Sequential: []string{"shipping"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("redirectSteps() = %+v, want %+v (must not drop the trailing Sequential step)", got, want)
	}
}

// TestRedirectStepsExclusiveBranch covers a deptID reachable only as an
// ExclusiveBranch's Target or RevertToDept.
func TestRedirectStepsExclusiveBranch(t *testing.T) {
	original := []dsl.ExecutionStep{
		{Exclusive: []dsl.ExclusiveBranch{
			{Target: "approved"},
			{RevertToDept: "rework"},
		}},
		{Sequential: []string{"shipping"}},
	}
	got := redirectSteps(original, "rework")
	want := []dsl.ExecutionStep{
		{Sequential: []string{"rework"}},
		{Sequential: []string{"shipping"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("redirectSteps() = %+v, want %+v", got, want)
	}
}

// TestRedirectStepsSubWorkflowNested covers a deptID reachable only inside a
// SubWorkflowStep's own inline Plan.Steps.
func TestRedirectStepsSubWorkflowNested(t *testing.T) {
	original := []dsl.ExecutionStep{
		{SubWorkflow: &dsl.SubWorkflowStep{
			Name: "escalation",
			Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"triage", "resolve"}},
			}},
		}},
		{Sequential: []string{"shipping"}},
	}
	got := redirectSteps(original, "resolve")
	want := []dsl.ExecutionStep{
		{Sequential: []string{"resolve"}},
		{Sequential: []string{"shipping"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("redirectSteps() = %+v, want %+v", got, want)
	}
}

// TestRedirectStepsNotFoundFallsBackToIsolation covers the one remaining
// structurally-unreachable case (a deptID that only exists inside a
// CallPool's separately compiled target plan, or a genuinely nonexistent
// deptID): redirectSteps can't recurse into it, so it isolates deptID —
// still not a data-loss bug on its own, since there's no trailing plan to
// preserve when the target isn't found anywhere in this plan's own tree.
func TestRedirectStepsNotFoundFallsBackToIsolation(t *testing.T) {
	original := []dsl.ExecutionStep{
		{CallPool: &dsl.CallPoolStep{Pool: "vendor-pool"}},
	}
	got := redirectSteps(original, "does-not-exist")
	want := []dsl.ExecutionStep{{Sequential: []string{"does-not-exist"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("redirectSteps() = %+v, want %+v", got, want)
	}
}
