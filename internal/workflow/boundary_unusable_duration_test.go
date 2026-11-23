package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"
)

func TestAddTimerCase_AnUnusableDurationLogsOneWarningCreatesNoTimerAndNothingFires(t *testing.T) {
	for _, duration := range []string{"5 minutes", "1d", "P1M", "abc", "0s", "-5m"} {
		t.Run(duration, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			logger := &w23aRecordingLogger{}
			suite.SetLogger(logger)
			env := suite.NewTestWorkflowEnvironment()
			var timers w23aTimerLog
			timers.attach(env)

			var fires int
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				sel := wf.NewSelector(ctx)
				cancel := addTimerCase(ctx, sel, duration, true, "dept", func(boundaryFire) { fires++ })
				sel.AddFuture(wf.NewTimer(ctx, 10*time.Hour), func(wf.Future) {})
				sel.Select(ctx)
				cancel()
				return nil
			})

			require.NoError(t, env.GetWorkflowError())
			assert.Zero(t, fires)
			require.Len(t, logger.messages(), 1)
			assert.Contains(t, logger.messages()[0], "disabling it")
			scheduled, _ := timers.snapshot()
			assert.Equal(t, []time.Duration{10 * time.Hour}, scheduled, "only the test's own sentinel timer exists: the stage waits with no deadline")
		})
	}
}
