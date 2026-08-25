package workflow

import (
	"context"
	"errors"
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
// plain Target dispatch when TargetNodeID is empty — regression protection
// for the pre-existing, non-ambiguous case (LLD §2.6's field table:
// "Dispatch uses TargetNodeID/RevertToNodeID when present, falling back to
// Target+TargetStage otherwise").
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
			{ConditionExpression: `decision == "approved"`, Target: "shipping", TargetStage: "approve"},
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
// control transfers to the boundary's TargetDept (LLD §2.2 step 5).
func TestRunTaskStageInterruptingBoundaryTransfers(t *testing.T) {
	env := newTestEnv()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

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

// TestRunCallPoolRecursesInlineWhenNotIgnored exercises CallPool's
// Ignored:false path — recurse inline like subProcess (LLD §2.3).
func TestRunCallPoolRecursesInlineWhenNotIgnored(t *testing.T) {
	env := newTestEnv()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "vendor", ToStage: "prep", ResultJSON: "{}"})
	}, time.Millisecond)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		target := &dsl.CompiledPlan{
			Name:        "vendor-pool",
			Ignored:     false,
			Departments: []dsl.DepartmentDef{{ID: "vendor", Stages: []dsl.StageDef{{Type: "prep"}}}},
			Execution:   dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"vendor"}}}},
		}
		collab := &dsl.CompiledCollaboration{MainPlan: "main", Plans: []*dsl.CompiledPlan{{Name: "main"}, target}}
		in := newInterpreter("tenant", "instance", "", collab, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })

		callerPlan := &dsl.CompiledPlan{Name: "main"}
		_, err := in.runCallPool(ctx, callerPlan, &dsl.CallPoolStep{Pool: "vendor-pool"}, admin)
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

// TestRunCallPoolTargetNotFound exercises the "target plan not found"
// error path.
func TestRunCallPoolTargetNotFound(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		collab := &dsl.CompiledCollaboration{MainPlan: "main", Plans: []*dsl.CompiledPlan{{Name: "main"}}}
		in := newInterpreter("tenant", "instance", "", collab, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		callerPlan := &dsl.CompiledPlan{Name: "main"}
		_, err := in.runCallPool(ctx, callerPlan, &dsl.CallPoolStep{Pool: "does-not-exist"}, admin)
		if err == nil {
			return errBadHistoryLen(-1)
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}
