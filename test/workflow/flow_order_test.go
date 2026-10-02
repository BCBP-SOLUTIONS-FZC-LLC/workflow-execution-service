package workflow_test

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl/dsltest"
)

// goldenFlowOrder is dsltest.FlowOrder as the activity hands it to the
// workflow.
func goldenFlowOrder(t *testing.T) *dsl.CompiledCollaboration {
	t.Helper()
	var c dsl.CompiledCollaboration
	if err := json.Unmarshal(dsltest.FlowOrder(), &c); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	expanded, err := dsl.ExpandCalls(&c, 0)
	if err != nil {
		t.Fatalf("expand golden: %v", err)
	}
	return expanded
}

// runFlowOrder completes each of tasks, node keys of the golden, with
// Approve's decision as given, and returns the node keys in the order their
// tasks were created.
func runFlowOrder(t *testing.T, decision string, tasks []string) []string {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var mu sync.Mutex
	var created []string
	registerFakeActivities(env, goldenFlowOrder(t), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			created = append(created, string(in.NodeKey))
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})
	for i, key := range tasks {
		dept, node, _ := strings.Cut(key, "/")
		result := "{}"
		if node == "Approve" {
			result = `{"decision":"` + decision + `"}`
		}
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: dept, NodeID: node, ToStage: "review", ResultJSON: result, RecordVersion: 1, VisitCount: 1,
			})
		}, time.Duration(i+1)*time.Minute)
	}

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error after creating %v: %v", created, err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v after creating %v, want COMPLETED", out.Status, created)
	}
	return created
}

// assertFlowOrder requires created to be want, except that the two parallel
// branches' tasks may be created in either order.
func assertFlowOrder(t *testing.T, created, want []string) {
	t.Helper()
	normalise := func(keys []string) []string {
		out := slices.Clone(keys)
		if i := slices.Index(out, "Ops~4/Bill"); i > 0 && out[i-1] == "Ops~3/Pack" {
			return out
		}
		if i := slices.Index(out, "Ops~3/Pack"); i > 0 && out[i-1] == "Ops~4/Bill" {
			out[i-1], out[i] = out[i], out[i-1]
		}
		return out
	}
	if got := normalise(created); !slices.Equal(got, want) {
		t.Errorf("tasks created in order %v, want %v", created, want)
	}
}

// The tasks run in the order the flows give, whatever lane they are in; the
// chosen branch runs its own work, a call included; and the two parallel
// branches in one lane both join.
func TestExecute_FlowOrder_RunsTasksInFlowOrder(t *testing.T) {
	for decision, want := range map[string][]string{
		"review": {
			"Engineering/Draft", "Ops/Check", "Engineering~2/Approve",
			"CA_Review::Process_Review/Review_Task", "CA_Review::Review_Check::Process_Check/Check_Task",
			"Ops~3/Pack", "Ops~4/Bill", "Engineering~3/Close",
		},
		"fix": {
			"Engineering/Draft", "Ops/Check", "Engineering~2/Approve",
			"Ops~2/Fix", "Ops~3/Pack", "Ops~4/Bill", "Engineering~3/Close",
		},
	} {
		t.Run(decision, func(t *testing.T) {
			assertFlowOrder(t, runFlowOrder(t, decision, want), want)
		})
	}
}

// A branch that leads to an end event ends its path the way the end event
// does: the instance completes, and its status is written, rather than being
// read as an admin's terminate.
func TestExecute_ExclusiveBranchToAnEndEvent_CompletesTheInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{MainPlan: "main", Plans: []*dsl.CompiledPlan{{
		Name: "main",
		Departments: []dsl.DepartmentDef{
			{ID: "Legal", Stages: []dsl.StageDef{{Type: "review", NodeID: "Review"}}},
			{ID: "Legal~2", Stages: []dsl.StageDef{{Type: "approve", NodeID: "Approve"}}},
		},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Sequential: []string{"Legal"}},
			{Exclusive: []dsl.ExclusiveBranch{
				{ConditionExpression: `decision == "reject"`, Terminates: true},
				{ConditionExpression: "", Target: "Legal~2", TargetNodeID: "Approve", Steps: []dsl.ExecutionStep{{Sequential: []string{"Legal~2"}}}, Terminates: true},
			}},
		}},
	}}}
	var created []string
	var written []domain.InstanceStatus
	registerFakeActivities(env, collab, &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			created = append(created, string(in.NodeKey))
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) { written = append(written, in.Status) },
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Legal", NodeID: "Review", ToStage: "review", ResultJSON: `{"decision":"reject"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, time.Minute)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted || !slices.Contains(written, domain.InstanceStatusCompleted) {
		t.Errorf("Status = %v, written %v; want COMPLETED, written", out.Status, written)
	}
	if !slices.Equal(created, []string{"Legal/Review"}) {
		t.Errorf("created %v, want Review alone", created)
	}
}
