package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

func TestFx11bAddTimerCaseIgnoresANonPositiveDuration(t *testing.T) {
	for _, duration := range []string{"0s", "-5s", "PT0S"} {
		t.Run(duration, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var fired bool
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				sel := wf.NewSelector(ctx)
				cancel := addTimerCase(ctx, sel, duration, true, "", func(boundaryFire) { fired = true })
				sel.AddFuture(wf.NewTimer(ctx, time.Hour), func(wf.Future) {})
				sel.Select(ctx)
				cancel()
				return nil
			})
			require.NoError(t, env.GetWorkflowError())
			assert.False(t, fired, "an interrupting boundary must not fire at once on %q", duration)
		})
	}
}
