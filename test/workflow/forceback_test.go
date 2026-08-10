package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
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
// TestExecute_InstanceCancelWhileRunning asserts cancel actually persists:
// CancelInstanceActivity — not the generic UpdateInstanceStatusActivity — is
// what must write the terminal state, since only it carries the
// task-FAILED-cascade/assignment-vacate contract (port.CancelInstanceInput's
// own doc comment). Previously this signal only flipped in-memory status;
// nothing asserted a persistence call happened at all.
func TestExecute_InstanceCancelWhileRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	var gotCancel port.CancelInstanceInput
	cancelCalls := 0
	var gotGenericStatus []port.UpdateInstanceStatusInput
	registerFakeActivities(env, collab, &activityHooks{
		cancelInstance: func(in port.CancelInstanceInput) error {
			gotCancel = in
			cancelCalls++
			return nil
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) {
			gotGenericStatus = append(gotGenericStatus, in)
		},
	})

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
	if cancelCalls != 1 {
		t.Fatalf("CancelInstanceActivity called %d times, want 1", cancelCalls)
	}
	if gotCancel.InstanceID != "instance-1" || gotCancel.TenantID != "tenant-1" {
		t.Errorf("CancelInstanceActivity input = %+v, want instance-1/tenant-1", gotCancel)
	}
	if gotCancel.AdminUserID != "admin-1" || gotCancel.RecordVersion != 1 {
		t.Errorf("CancelInstanceActivity input = %+v, want admin-1/recordVersion=1 from the signal", gotCancel)
	}
	if len(gotGenericStatus) != 0 {
		t.Errorf("UpdateInstanceStatusActivity called %d times, want 0 — CancelInstanceActivity already wrote the terminal state, a second generic write would be redundant/spurious", len(gotGenericStatus))
	}
}

// TestExecute_InstanceCancelActivityErrorPropagates asserts a non-retryable
// CancelInstanceActivity failure surfaces as the workflow's own error
// (finalStatus falls back to FAILED via Execute's runErr handling) rather
// than being silently discarded — cancel's write is the authoritative
// terminal state, unlike pause/resume's/recordForceRoute's best-effort audit
// writes, so a failure here must not be reported as a successful Terminate.
func TestExecute_InstanceCancelActivityErrorPropagates(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	registerFakeActivities(env, collab, &activityHooks{
		cancelInstance: func(port.CancelInstanceInput) error {
			return temporal.NewApplicationError("stale record_version", "ValidationError")
		},
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-cancel:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow returned no error, want CancelInstanceActivity's failure to propagate")
	}
}

// TestExecute_InstancePauseAndResumePersist asserts pause/resume actually
// persist to DB and write the matching event via PauseInstanceActivity/
// ResumeInstanceActivity — previously these signals only flipped in-memory
// status, so a paused instance's DB row would report RUNNING indefinitely.
func TestExecute_InstancePauseAndResumePersist(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	var gotPause port.PauseInstanceInput
	var gotResume port.ResumeInstanceInput
	pauseCalls, resumeCalls := 0, 0
	registerFakeActivities(env, collab, &activityHooks{
		pauseInstance: func(in port.PauseInstanceInput) {
			gotPause = in
			pauseCalls++
		},
		resumeInstance: func(in port.ResumeInstanceInput) {
			gotResume = in
			resumeCalls++
		},
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-pause:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-resume:instance-1", adminSignalWire{AdminUserID: "admin-2", RecordVersion: 2})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-cancel:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 3})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if pauseCalls != 1 {
		t.Fatalf("PauseInstanceActivity called %d times, want 1", pauseCalls)
	}
	if gotPause.InstanceID != "instance-1" || gotPause.TenantID != "tenant-1" || gotPause.AdminUserID != "admin-1" || gotPause.RecordVersion != 1 {
		t.Errorf("PauseInstanceActivity input = %+v, want instance-1/tenant-1/admin-1/recordVersion=1", gotPause)
	}
	if resumeCalls != 1 {
		t.Fatalf("ResumeInstanceActivity called %d times, want 1", resumeCalls)
	}
	if gotResume.InstanceID != "instance-1" || gotResume.TenantID != "tenant-1" || gotResume.AdminUserID != "admin-2" || gotResume.RecordVersion != 2 {
		t.Errorf("ResumeInstanceActivity input = %+v, want instance-1/tenant-1/admin-2/recordVersion=2", gotResume)
	}
}

// TestExecute_InstanceReassignCallsActivity asserts an instance-reassign
// signal reaches ReassignAssignmentActivity with the right input — the
// node-override HTTP path's eventual runtime counterpart (LLD §3.1),
// entirely unwired until now.
func TestExecute_InstanceReassignCallsActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	var got port.ReassignAssignmentInput
	calls := 0
	registerFakeActivities(env, collab, &activityHooks{
		reassignAssignment: func(in port.ReassignAssignmentInput) error {
			got = in
			calls++
			return nil
		},
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-reassign:instance-1", reassignSignalWire{
			TaskID: "task-1", OldUserID: "user-1", NewUserID: "user-2", AdminUserID: "admin-1", RecordVersion: 1,
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptB", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("ReassignAssignmentActivity called %d times, want 1", calls)
	}
	if got.TaskID != "task-1" || got.TenantID != "tenant-1" || got.OldUserID != "user-1" ||
		got.NewUserID != "user-2" || got.AdminUserID != "admin-1" || got.RecordVersion != 1 {
		t.Errorf("ReassignAssignmentActivity input = %+v, want task-1/tenant-1/user-1/user-2/admin-1/recordVersion=1", got)
	}
}

// TestExecute_InstanceReassignRejectedWhilePaused asserts the signal
// precondition gate applies to instance-reassign the same way it does to
// every other signal (LLD §7.2 test #5): a PAUSED instance drops it rather
// than calling ReassignAssignmentActivity.
func TestExecute_InstanceReassignRejectedWhilePaused(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	calls := 0
	registerFakeActivities(env, collab, &activityHooks{
		reassignAssignment: func(port.ReassignAssignmentInput) error {
			calls++
			return nil
		},
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-pause:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-reassign:instance-1", reassignSignalWire{
			TaskID: "task-1", OldUserID: "user-1", NewUserID: "user-2", AdminUserID: "admin-1", RecordVersion: 1,
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-cancel:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 2})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if calls != 0 {
		t.Fatalf("ReassignAssignmentActivity called %d times, want 0 (instance was PAUSED)", calls)
	}
}

// TestExecute_StageDeferPopsHistoryAndRegresses drives the stage-defer
// signal (LLD §3.1): a user-initiated single-step regress distinct from
// admin force-back, backed by DeferTaskActivity.
func TestExecute_StageDeferPopsHistoryAndRegresses(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := twoDeptCollaboration()
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	// Defer regresses deptA — the instance stays RUNNING throughout, no
	// redirect happens (stage-defer only pops history/resets the message
	// buffer and records the defer; it doesn't re-dispatch on its own).
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-defer:instance-1", struct {
			DeptID        string
			FromStage     string
			Reason        string
			UserID        string
			RecordVersion int64
		}{DeptID: "deptA", FromStage: "prep", Reason: "need more info", UserID: "user-1", RecordVersion: 1})
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
