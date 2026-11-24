package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// Two inner forks fail, so two parks are open together. An instance-cancel is
// handed to one park and the other is told to stop; the cancel write must still
// run once, so the instance row is not left DEGRADED.
func TestExecute_CancelWithTwoParksOpenRunsTheCancelWriteOnce(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && (v.NodeKey == "x2/prep" || v.NodeKey == "y2/prep") {
			return wflRefuse("create " + string(v.NodeKey))
		}
		return nil
	}
	wflRegisterAll(env, fakes, twoInnerForksCollaboration())
	wflSignalsAt(env, 2*time.Second,
		wflSignal{"stage-transition", wflDoneStage("x1", "prep", 1)},
		wflSignal{"stage-transition", wflDoneStage("y1", "prep", 1)},
	)
	wflCancelAt(env, 30*time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	statuses := wflStatuses(fakes)
	require.NotEmpty(t, statuses)
	assert.Equal(t, 2, countStatus(statuses, domain.InstanceStatusDegraded), "both parks were written DEGRADED")
}

func countStatus(all []domain.InstanceStatus, want domain.InstanceStatus) int {
	n := 0
	for _, s := range all {
		if s == want {
			n++
		}
	}
	return n
}
