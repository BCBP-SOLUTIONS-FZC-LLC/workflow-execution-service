package wflclock_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
)

func sleepingWorkflow(ctx wf.Context) error {
	return wf.Sleep(ctx, time.Hour)
}

func newEnv() *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(sleepingWorkflow)
	return env
}

func TestAfter_ASecondCallbackAtTheSameInstantIsRefused(t *testing.T) {
	env := newEnv()
	wflclock.After(env, func() {}, 5*time.Second)

	assert.PanicsWithValue(t,
		"wflclock: two callbacks are due 5s after the start; move one by a tick or deliver them with Together",
		func() { wflclock.After(env, func() {}, 5*time.Second) })
}

func TestAfter_CallbacksAtDifferentInstantsRunInInstantOrder(t *testing.T) {
	env := newEnv()
	var ran []string
	wflclock.After(env, func() { ran = append(ran, "late") }, 7*time.Second)
	wflclock.After(env, func() { ran = append(ran, "early") }, 3*time.Second)

	env.ExecuteWorkflow(sleepingWorkflow)

	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, []string{"early", "late"}, ran)
}

func TestAfter_ACallbackScheduledFromACallbackCountsFromItsOwnInstant(t *testing.T) {
	env := newEnv()
	var ran []string
	wflclock.After(env, func() {
		ran = append(ran, "outer")
		wflclock.After(env, func() { ran = append(ran, "inner") }, 4*time.Second)
	}, 2*time.Second)
	wflclock.After(env, func() { ran = append(ran, "third") }, 9*time.Second)

	env.ExecuteWorkflow(sleepingWorkflow)

	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, []string{"outer", "inner", "third"}, ran)
	assert.Panics(t, func() { wflclock.After(env, func() {}, 9*time.Second) })
}

func TestTogether_DeliversInTheGivenOrderFromOneCallback(t *testing.T) {
	env := newEnv()
	var ran []string
	wflclock.Together(env, 5*time.Second,
		func() { ran = append(ran, "first") },
		func() { ran = append(ran, "second") },
		func() { ran = append(ran, "third") },
	)

	env.ExecuteWorkflow(sleepingWorkflow)

	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, []string{"first", "second", "third"}, ran)
}

func TestQueued_RunsAfterTheCallbacksAlreadyQueued(t *testing.T) {
	env := newEnv()
	var ran []string
	wflclock.After(env, func() {
		env.SignalWorkflow("first", nil)
		wflclock.Queued(env, func() { ran = append(ran, "queued") })
		ran = append(ran, "callback")
	}, time.Second)

	env.ExecuteWorkflow(sleepingWorkflow)

	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, []string{"callback", "queued"}, ran)
}

func TestNoTestRegistersADelayedCallbackDirectly(t *testing.T) {
	root := filepath.Join("..", "..")
	var offenders []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "vendor" || d.Name() == ".tools" || d.Name() == ".git" || d.Name() == "wflclock") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(src), ".RegisterDelayedCallback(") {
			offenders = append(offenders, path)
		}
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, offenders, "schedule callbacks with wflclock.After or wflclock.Together, which refuse two callbacks at one instant")
}
