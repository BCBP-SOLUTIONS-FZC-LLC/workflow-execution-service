package workflow

import (
	"slices"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// TestBranchKeys pins that branches are keyed by place, so two branches of
// the same department join separately.
func TestBranchKeys(t *testing.T) {
	got := branchKeys([]dsl.ParallelBranch{{DeptID: "Ops"}, {DeptID: "Ops"}})
	if want := []string{"0", "1"}; !slices.Equal(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
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
