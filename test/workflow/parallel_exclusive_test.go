package workflow_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestExecute_ParallelBranchesResolveTheirOwnExclusiveGate exercises two
// Parallel branches, each completing its own department and then hitting its
// own Exclusive gateway keyed to a different decision value. Both branches'
// stage-transition signals are delivered at the same instant — the
// adversarial timing a sibling branch's own result must never leak across.
// Each branch's Exclusive gate must route on its own decision, never the
// other branch's.
func TestExecute_ParallelBranchesResolveTheirOwnExclusiveGate(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "deptA", Stages: []dsl.StageDef{{Type: "prep"}}},
				{ID: "deptA2", Stages: []dsl.StageDef{{Type: "prep"}}},
				{ID: "deptB", Stages: []dsl.StageDef{{Type: "prep"}}},
				{ID: "deptB2", Stages: []dsl.StageDef{{Type: "prep"}}},
			},
			Execution: dsl.ExecutionPlan{
				Steps: []dsl.ExecutionStep{
					{Parallel: []dsl.ParallelBranch{
						{DeptID: "deptA", Steps: []dsl.ExecutionStep{
							{Sequential: []string{"deptA"}},
							{Exclusive: []dsl.ExclusiveBranch{
								{ConditionExpression: `decision == "alpha"`, Target: "deptA2"},
							}},
						}},
						{DeptID: "deptB", Steps: []dsl.ExecutionStep{
							{Sequential: []string{"deptB"}},
							{Exclusive: []dsl.ExclusiveBranch{
								{ConditionExpression: `decision == "beta"`, Target: "deptB2"},
							}},
						}},
					}},
				},
			},
		}},
	}

	var mu sync.Mutex
	var createdNodeKeys []domain.NodeKey
	registerFakeActivities(env, collab, &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		mu.Lock()
		createdNodeKeys = append(createdNodeKeys, in.NodeKey)
		mu.Unlock()
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}})

	// Both branches' first-stage results arrive at the same instant — the
	// adversarial case: each branch's own Exclusive gate must still resolve
	// against its own decision, not whichever signal the router happened to
	// process first.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptB", ToStage: "prep", ResultJSON: `{"decision":"beta"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA", ToStage: "prep", ResultJSON: `{"decision":"alpha"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptA2", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "deptB2", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
		})
	}, 2*time.Millisecond)

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

	mu.Lock()
	defer mu.Unlock()
	var sawA2, sawB2 bool
	for _, k := range createdNodeKeys {
		if k == "deptA2/prep" {
			sawA2 = true
		}
		if k == "deptB2/prep" {
			sawB2 = true
		}
	}
	if !sawA2 {
		t.Errorf("createdNodeKeys = %v, want deptA2/prep present — deptA's own Exclusive gate must route on its own \"alpha\" decision", createdNodeKeys)
	}
	if !sawB2 {
		t.Errorf("createdNodeKeys = %v, want deptB2/prep present — deptB's own Exclusive gate must route on its own \"beta\" decision", createdNodeKeys)
	}
}
