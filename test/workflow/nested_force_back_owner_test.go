package workflow_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func stepBeforeInnerForkCollaboration() *dsl.CompiledCollaboration {
	dept := func(id string) dsl.DepartmentDef {
		return dsl.DepartmentDef{ID: id, Stages: []dsl.StageDef{{Type: "prep", Activity: "work"}}}
	}
	leaf := func(id string) dsl.ParallelBranch {
		return dsl.ParallelBranch{DeptID: id, Steps: []dsl.ExecutionStep{{Sequential: []string{id}}}}
	}
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: []dsl.DepartmentDef{dept("intake"), dept("a"), dept("pre"), dept("b1"), dept("b2")},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Sequential: []string{"intake"}},
				{Parallel: []dsl.ParallelBranch{
					leaf("a"),
					{DeptID: "b", Steps: []dsl.ExecutionStep{
						{Sequential: []string{"pre"}},
						{Parallel: []dsl.ParallelBranch{leaf("b1"), leaf("b2")}},
					}},
				}},
			}},
		}},
	}
}

// A force-back goes to the innermost fork that holds an open branch: the step
// before the inner fork runs again, and the outer fork's other branch keeps
// its task.
func TestExecute_ForceBackInANestedParallelGoesToTheInnermostFork(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var mu sync.Mutex
	created := map[domain.NodeKey]int{}
	registerFakeActivities(env, stepBeforeInnerForkCollaboration(), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			created[in.NodeKey]++
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})

	at := func(d time.Duration, dept string, visit int64) {
		wflclock.After(env, func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: dept, ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: visit,
			})
		}, d)
	}
	at(tick, "intake", 1)
	at(5*tick, "pre", 1)
	wflclock.After(env, func() {
		env.SignalWorkflow("instance-force-back:instance-1", adminSignalWire{
			AdminUserID: "admin-1", RecordVersion: 1,
		})
	}, 10*tick)
	at(15*tick, "pre", 2)
	at(20*tick, "a", 1)
	at(25*tick, "b1", 1)
	at(30*tick, "b2", 1)

	out := finish(t, env, "instance-1")

	mu.Lock()
	defer mu.Unlock()
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
	if created["pre/prep"] != 2 {
		t.Errorf("tasks created = %v, want pre twice: the inner fork returns to the step before it", created)
	}
	if created["intake/prep"] != 1 {
		t.Errorf("tasks created = %v, want intake once: the outer fork was not sent back", created)
	}
}
