package workflow

import (
	"fmt"
	"testing"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestCurrentPendingNode_IsTheSameNodeOnEveryCallWhenADepartmentHasSeveral(t *testing.T) {
	in := &interpreter{pending: map[nodeVisitKey]wf.Channel{}}
	for i := 0; i < 12; i++ {
		in.pending[nodeVisitKey{Node: domain.NodeKey(fmt.Sprintf("billing/step_%02d", i)), Visit: 1}] = nil
	}
	in.pending[nodeVisitKey{Node: "warehouse/pack", Visit: 1}] = nil

	for i := 0; i < 50; i++ {
		if got := in.currentPendingNode("billing"); got != "billing/step_00" {
			t.Fatalf("call %d = %q, want the smallest pending node of the department, billing/step_00: map order must not decide the audit's old node keys", i, got)
		}
	}
	if got := in.currentPendingNode("nobody"); got != "" {
		t.Errorf("a department with nothing pending = %q, want empty", got)
	}
}
