package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func twoDeptCollaboration() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "deptA", Stages: []dsl.StageDef{{Type: "prep", Activity: "step_a"}}},
					{ID: "deptB", Stages: []dsl.StageDef{{Type: "prep", Activity: "step_b"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{Sequential: []string{"deptA", "deptB"}},
					},
				},
			},
		},
	}
}

// TestExecute_BaseForceBackRegressesAndContinues drives the non-parallel
// force-back mechanism (LLD §2.7's base case): once deptA completes and
// deptB is pending, instance-force-back regresses to deptA — which then
// re-runs deptA followed by the REST of the original plan (deptB), not
// deptA in isolation.
func TestExecute_BaseForceBackRegressesAndContinues(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "deptA", TargetNodeKey: "deptA/prep", RecordVersion: 1,
		})
	}, 5*time.Millisecond)

	// Regressed deptA must be completed again.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 10*time.Millisecond)

	// The plan must still continue on to deptB afterward.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptB", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 15*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED (force-back must not skip deptB)", out.Status)
	}
}

func threeDeptCollaboration() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "deptA", Stages: []dsl.StageDef{{Type: "prep", Activity: "step_a"}}},
					{ID: "deptB", Stages: []dsl.StageDef{{Type: "prep", Activity: "step_b"}}},
					{ID: "deptC", Stages: []dsl.StageDef{{Type: "prep", Activity: "step_c"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{Sequential: []string{"deptA", "deptB", "deptC"}},
					},
				},
			},
		},
	}
}

// TestExecute_BaseForceForwardSkipsWithoutWipingHistory drives the base
// instance-force-forward path: deptA completes, deptB's task is created and
// left pending (the realistic trigger for an admin force-forward — skipping
// a stuck task), then force-forward jumps straight to deptC. deptB's task
// must never be COMPLETED (bypassed, not worked), and — unlike force-back —
// deptA's completed history must survive intact, since force-forward
// targets a node that hasn't been visited yet and is not a regression
// (LLD §3.1's "jump beyond the compiled graph's explicit edges").
func TestExecute_BaseForceForwardSkipsWithoutWipingHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := threeDeptCollaboration()
	var recordedOldNodeKeys []string
	registerFakeActivities(env, collab, &activityHooks{
		recordForceRoute: func(in port.RecordForceRouteInput) {
			for _, k := range in.OldNodeKeys {
				recordedOldNodeKeys = append(recordedOldNodeKeys, string(k))
			}
		},
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "deptC", TargetNodeKey: "deptC/prep", RecordVersion: 1,
		})
	}, 5*time.Millisecond)

	// Deliberately never signal deptB:prep's completion — if the
	// implementation regressed to waiting on deptB instead of skipping to
	// deptC, this test would hang until Temporal's test environment's
	// default execution timeout, not merely fail an assertion.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptC", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 10*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if len(recordedOldNodeKeys) != 1 || recordedOldNodeKeys[0] != "deptA/prep" {
		t.Errorf("RecordForceRouteActivity's OldNodeKeys = %v, want exactly [deptA/prep] (the bypassed position, not the whole history)", recordedOldNodeKeys)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}

// TestExecute_InstanceCancelWhileRunning drives the base instance-cancel
// path: cancelling while simply RUNNING (no Parallel gateway active, not
// DEGRADED) terminates the instance immediately regardless of the pending
// task.
func TestExecute_InstanceCancelWhileRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-cancel:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusTerminated {
		t.Errorf("Status = %v, want TERMINATED", out.Status)
	}
}

// TestExecute_StageDeferOnPendingStageDoesNotDisruptFlow drives the
// stage-defer signal (LLD §3.1) against the currently-pending stage it's
// meant for — a user-initiated defer of their own in-flight task, backed by
// DeferTaskActivity — and confirms it doesn't perturb the interpreter's own
// control flow (history/message-buffer/dispatch) at all: that bookkeeping is
// DeferTaskActivity's own concern, not the signal handler's, since the
// pending stage was never pushed to history in the first place (stage.go
// only pushes once a stage completes).
func TestExecute_StageDeferOnPendingStageDoesNotDisruptFlow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	// deptB/prep is now pending — defer it directly, the real usage shape.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-defer:instance-1", struct {
			DeptID        string
			FromStage     string
			Reason        string
			UserID        string
			RecordVersion int64
		}{DeptID: "deptB", FromStage: "prep", Reason: "need more info", UserID: "user-1", RecordVersion: 1})
	}, 5*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptB", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 10*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}

// TestExecute_ActiveParallelForceBackPausesAndResumes drives LLD §2.7's
// active-parallel-gateway extension: force-back arriving while Parallel
// branches are still active (none failed yet) regresses to the pre-fork
// entry and resumes the branches afterward, rather than cancelling them.
//
// Note: pauseDept/checkPaused only take effect at a stage boundary
// (types.go's checkPaused doc comment); both single-stage branches here are
// already inside their own task wait when force-back fires, so this test
// exercises the regression-and-resume outcome, not the pause gate itself
// actually blocking a branch mid-stage-boundary.
func TestExecute_ActiveParallelForceBackPausesAndResumes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "intake", Stages: []dsl.StageDef{{Type: "prep", Activity: "intake_order"}}},
					{ID: "warehouse", Stages: []dsl.StageDef{{Type: "prep", Activity: "pack_order"}}},
					{ID: "billing", Stages: []dsl.StageDef{{Type: "prep", Activity: "charge_card"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{Sequential: []string{"intake"}},
						{Parallel: []dsl.ParallelBranch{
							{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
							{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
						}},
					},
				},
			},
		},
	}
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "intake", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	// Fires while both warehouse and billing are still pending (neither has
	// settled) — the active-parallel-gateway extension, not DEGRADED.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "intake", TargetNodeKey: "intake/prep", RecordVersion: 1,
		})
	}, 5*time.Millisecond)

	// Regressed intake must be completed again before the branches resume.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "intake", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 10*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 15*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "billing", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 20*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult error: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}
