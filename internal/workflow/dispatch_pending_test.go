package workflow

import (
	"fmt"
	"slices"
	"testing"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestPendingNodesIn_ListsOnlyTheBranchsNodesInTheSameOrderOnEveryCall(t *testing.T) {
	in := &interpreter{pending: map[nodeVisitKey]wf.Channel{}}
	var want []domain.NodeKey
	for i := 0; i < 12; i++ {
		key := domain.NodeKey(fmt.Sprintf("billing/step_%02d", i))
		in.pending[nodeVisitKey{Node: key, Visit: 1}] = nil
		want = append(want, key)
	}
	in.pending[nodeVisitKey{Node: "warehouse/pack", Visit: 1}] = nil
	steps := []dsl.ExecutionStep{{Sequential: []string{"billing"}}}

	for i := 0; i < 50; i++ {
		if got := in.pendingNodesIn(steps); !slices.Equal(got, want) {
			t.Fatalf("call %d = %v, want the branch's own pending nodes sorted, %v: map order must not decide the audit's old node keys", i, got, want)
		}
	}
	if got := in.pendingNodesIn([]dsl.ExecutionStep{{Sequential: []string{"nobody"}}}); len(got) != 0 {
		t.Errorf("a branch with nothing pending = %v, want empty", got)
	}
}
