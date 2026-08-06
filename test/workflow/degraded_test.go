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

func parallelCollaboration() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "warehouse", Stages: []dsl.StageDef{{Type: "prep", Activity: "pack_order"}}},
					{ID: "billing", Stages: []dsl.StageDef{{Type: "prep", Activity: "charge_card"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{Parallel: []dsl.ParallelBranch{
							{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
							{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
						}},
					},
				},
			},
		},
	}
}

// TestExecute_ParallelBranchFailureDegradesThenForceForwardResolves drives
// LLD §3.3's 9-step DEGRADED procedure: one Parallel branch's activity
// exhausts retries non-retryably, the other completes normally, the
// instance parks in DEGRADED once both have settled, and an
// instance-force-forward signal resolves the failed branch — the instance
// returns to RUNNING and then completes.
func TestExecute_ParallelBranchFailureDegradesThenForceForwardResolves(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing/prep" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
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

// TestExecute_ParallelBranchFailureDegradesThenForceBackRespawns drives the
// respawn path: instance-force-back on a failed branch spawns a brand-new
// goroutine for that department from scratch (LLD Appendix A.2 #8/#9),
// which this time succeeds, resolving DEGRADED back to RUNNING.
func TestExecute_ParallelBranchFailureDegradesThenForceBackRespawns(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	firstAttempt := true
	billingCreateTaskCalls := 0
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey != "billing/prep" {
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		}
		billingCreateTaskCalls++
		if firstAttempt {
			firstAttempt = false
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "billing", RecordVersion: 1,
		})
	}, 10*time.Millisecond)

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
	if billingCreateTaskCalls != 2 {
		t.Errorf("billing CreateTaskActivity calls = %d, want 2 (initial failure + respawn) — a respawn that never actually re-runs the department would pass this test's Status assertion alone", billingCreateTaskCalls)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}

// TestExecute_ActiveParallelForceForwardSupersedesOneBranch closes the T1.1
// gap: instance-force-forward's LLD §3.1 precondition is "RUNNING or
// DEGRADED", not DEGRADED-only — force-forward must resolve a still-active
// Parallel branch (neither branch has failed, the instance never enters
// DEGRADED at all) exactly like it already does for a failed one. billing is
// force-forwarded away and never signalled to complete; only warehouse's own
// completion is needed for the instance to finish.
func TestExecute_ActiveParallelForceForwardSupersedesOneBranch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	var recordedOldNodeKeys []string
	forceRouteCalls := 0
	sawDegraded := false
	registerFakeActivities(env, collab, &activityHooks{
		recordForceRoute: func(in port.RecordForceRouteInput) {
			forceRouteCalls++
			for _, k := range in.OldNodeKeys {
				recordedOldNodeKeys = append(recordedOldNodeKeys, string(k))
			}
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) {
			if in.Status == domain.InstanceStatusDegraded {
				sawDegraded = true
			}
		},
	})

	// Fires while both warehouse and billing are still pending (neither has
	// settled) — the gap this test closes, not DEGRADED's own force-forward.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
		})
	}, 5*time.Millisecond)

	// Deliberately never signal billing/prep's completion — if the
	// implementation fell back to the old drop-and-log behavior, this test
	// would hang until the test environment's default execution timeout,
	// not merely fail an assertion.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
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
	if sawDegraded {
		t.Error("instance entered DEGRADED; force-forward on a still-active branch must resolve without ever degrading")
	}
	if forceRouteCalls != 1 {
		t.Errorf("RecordForceRouteActivity calls = %d, want 1", forceRouteCalls)
	}
	if len(recordedOldNodeKeys) != 1 || recordedOldNodeKeys[0] != "billing/prep" {
		t.Errorf("RecordForceRouteActivity's OldNodeKeys = %v, want exactly [billing/prep]", recordedOldNodeKeys)
	}
}

// TestExecute_ActiveParallelForceForwardDuplicateSignalIsNoOp drives the
// resolved-guard added alongside the fix above: a second force-forward for a
// dept already resolved (whether a genuine duplicate delivery or simply
// late) must be dropped, not processed again.
func TestExecute_ActiveParallelForceForwardDuplicateSignalIsNoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	forceRouteCalls := 0
	registerFakeActivities(env, collab, &activityHooks{
		recordForceRoute: func(port.RecordForceRouteInput) { forceRouteCalls++ },
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
		})
	}, 5*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
		})
	}, 6*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
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
	if forceRouteCalls != 1 {
		t.Errorf("RecordForceRouteActivity calls = %d, want exactly 1 (duplicate force-forward must be dropped)", forceRouteCalls)
	}
}

// TestExecute_DEGRADEDRejectsInstancePause is the workflow-level companion
// to LLD §7.2 test #5's unit-level validateSignal coverage: while an
// instance is parked in DEGRADED, an instance-pause signal must be rejected
// at signal validation and never actually pause the instance — the
// subsequent force-forward must still resolve DEGRADED normally,
// demonstrating the pause signal had no effect at all.
func TestExecute_DEGRADEDRejectsInstancePause(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := parallelCollaboration()
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing/prep" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1,
		})
	}, time.Millisecond)

	// Arrives while DEGRADED: instance-pause requires RUNNING, so this must
	// be rejected at signal validation before it ever reaches the park
	// loop's Selector (which registers no case for it at all).
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-pause:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, 5*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("instance-force-forward:instance-1", adminSignalWire{
			AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
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
		t.Errorf("Status = %v, want COMPLETED (instance-pause must not have parked the instance)", out.Status)
	}
}
