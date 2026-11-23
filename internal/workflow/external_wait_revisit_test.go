package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

func TestWaitExternal_AWaitOnAKeyAfterTheEarlierOneWasCancelledIsTheOneRegistered(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		first, cancelFirst := in.waitExternal(ctx, "finance/Receive_Bid")
		cancelFirst()
		_, registered := in.externalWaiting["finance/Receive_Bid"]
		assert.False(t, registered, "a cancelled wait leaves no entry")

		second, cancelSecond := in.waitExternal(ctx, "finance/Receive_Bid")
		assert.NotSame(t, first, second)
		assert.Same(t, second, in.externalWaiting["finance/Receive_Bid"])
		cancelFirst()
		assert.Same(t, second, in.externalWaiting["finance/Receive_Bid"], "cancelling the earlier wait again does not remove the later one")
		cancelSecond()
		assert.Empty(t, in.externalWaiting)
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
}
