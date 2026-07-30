package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestExecute_SubWorkflowRunsInline drives a subProcess step: it recurses
// into its nested ExecutionPlan inline, in the same Temporal workflow
// execution — never a child workflow (LLD §2.3) — and completes normally
// once its inner department's stage resolves.
func TestExecute_SubWorkflowRunsInline(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "review", Stages: []dsl.StageDef{{Type: "review", Activity: "review_order"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{SubWorkflow: &dsl.SubWorkflowStep{
							NodeID: "sub1",
							Name:   "review-subprocess",
							Plan: dsl.ExecutionPlan{
								Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}},
							},
						}},
					},
				},
			},
		},
	}
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "review", ToStage: "review", ResultJSON: "{}", RecordVersion: 1,
		})
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
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
}

// TestExecute_SubWorkflowInterruptingTimerBoundary drives a subProcess whose
// timer boundary fires before the inner stage resolves: an interrupting
// timer cancels the in-flight subprocess and transfers control to the
// boundary's TargetDept (LLD §2.2).
func TestExecute_SubWorkflowInterruptingTimerBoundary(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	env.SetStartTime(start)

	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "review", Stages: []dsl.StageDef{{Type: "review", Activity: "review_order"}}},
					{ID: "escalation", Stages: []dsl.StageDef{{Type: "approve", Activity: "escalate_order"}}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{SubWorkflow: &dsl.SubWorkflowStep{
							NodeID: "sub1",
							Name:   "review-subprocess",
							Plan: dsl.ExecutionPlan{
								Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}},
							},
							TimerPaths: []dsl.TimerPath{
								{Duration: "1h", Interrupting: true, TargetDept: "escalation"},
							},
						}},
					},
				},
			},
		},
	}
	registerFakeActivities(env, collab, nil)

	// The review stage never resolves — the timer boundary must fire
	// instead and transfer control to escalation.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "escalation", ToStage: "approve", ResultJSON: "{}", RecordVersion: 1,
		})
	}, 2*time.Hour)

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
		t.Errorf("Status = %v, want COMPLETED (via the escalation boundary target)", out.Status)
	}
}
