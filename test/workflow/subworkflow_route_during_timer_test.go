package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// A force-forward that skips a parallel branch whose only step is a call with
// an armed timer path cancels the call cleanly: the other branch finishes and
// the instance completes.
func TestExecute_ForceForwardOfAParallelBranchHoldingACallWithAnArmedTimerPathCompletes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflCallOf([]dsl.TimerPath{{Duration: "1h", Interrupting: true, TargetDept: "esc"}}, nil)
	plan := collab.Plans[0]
	call := plan.Execution.Steps[0]
	plan.Execution.Steps = []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
		{DeptID: "review", Steps: []dsl.ExecutionStep{call}},
		{DeptID: "after", Steps: []dsl.ExecutionStep{{Sequential: []string{"after"}}}},
	}}}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 10*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-5", TargetDeptID: "review", TargetNodeKey: "review/Review", RecordVersion: 3})
	wflSignalAt(env, 20*time.Second, "stage-transition", wflDone("after", "Finish", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.ElementsMatch(t, []domain.NodeKey{"review/Review", "after/Finish"}, wflNodesCreated(fakes), "the timer path never started")
}
