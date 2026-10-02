package workflow

import (
	"slices"
	"testing"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestBranchKeys pins that branches are keyed by place, and, in a history
// recorded before parallelJoinByBranchChangeID, by DeptID as they were.
func TestBranchKeys(t *testing.T) {
	branches := []dsl.ParallelBranch{{DeptID: "Ops"}, {DeptID: "Ops"}}
	for _, tc := range []struct {
		version wf.Version
		want    []string
	}{{1, []string{"0", "1"}}, {wf.DefaultVersion, []string{"Ops", "Ops"}}} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.OnGetVersion(parallelJoinByBranchChangeID, wf.DefaultVersion, 1).Return(tc.version)
		var got []string
		env.ExecuteWorkflow(func(ctx wf.Context) error {
			got = branchKeys(ctx, branches)
			return nil
		})
		if !slices.Equal(got, tc.want) {
			t.Errorf("version %d: keys = %v, want %v", tc.version, got, tc.want)
		}
	}
}

// TestBranchRunning pins how a force-forward finds its branch: by key, or by
// a department the branch's steps run further in, never a resolved branch.
func TestBranchRunning(t *testing.T) {
	keys := []string{"0", "1"}
	steps := map[string][]dsl.ExecutionStep{
		"0": {{Exclusive: []dsl.ExclusiveBranch{{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops~2"}}}}}}},
		"1": {{Sequential: []string{"Eng"}}, {SubWorkflow: &dsl.SubWorkflowStep{Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"CA::Rev"}}}}}}},
	}
	for dept, want := range map[string]string{"Ops~2": "0", "CA::Rev": "1", "Legal": ""} {
		if got := branchRunning(keys, steps, map[string]bool{}, dept); got != want {
			t.Errorf("branchRunning(%s) = %q, want %q", dept, got, want)
		}
	}
	if got := branchRunning(keys, steps, map[string]bool{"0": true}, "Ops~2"); got != "" {
		t.Errorf("branchRunning found resolved branch %q", got)
	}
}
