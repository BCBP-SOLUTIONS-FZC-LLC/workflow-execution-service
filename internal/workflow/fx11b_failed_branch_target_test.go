package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFx11bFailedBranchForAnEmptyTargetNeedsASingleFailedBranch(t *testing.T) {
	neverCompleted := failedBranch{DeptID: "b", LastCompletedNode: ""}
	completed := failedBranch{DeptID: "a", LastCompletedNode: "a/review"}

	cases := []struct {
		name   string
		failed []failedBranch
		sig    adminSignal
		want   int
	}{
		{"one failed branch is the target", []failedBranch{neverCompleted}, adminSignal{}, 0},
		{"two failed, no target, one never completed a node", []failedBranch{completed, neverCompleted}, adminSignal{}, -1},
		{"two failed, no target, the never-completed one first", []failedBranch{neverCompleted, completed}, adminSignal{}, -1},
		{"two failed, named target", []failedBranch{completed, neverCompleted}, adminSignal{TargetDeptID: "b"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, failedBranchFor(tc.failed, tc.sig))
		})
	}
}
